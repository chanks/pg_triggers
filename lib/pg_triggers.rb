require "pg_triggers/version"

module PgTriggers
  class << self
    def counter_cache(main_table, counter_column, counted_table, relationship, options = {})
      where   = proc { |source| relationship.map{|k, v| "#{k} = #{source}.#{v}"}.join(' AND ') }
      columns = relationship.values
      changed = columns.map{|c| "((OLD.#{c} <> NEW.#{c}) OR (OLD.#{c} IS NULL <> NEW.#{c} IS NULL))"}.join(' OR ')

      condition = proc do |source|
        a = []
        a << columns.map{|c| "#{source}.#{c} IS NOT NULL"}.join(' AND ')
        a << options[:where].gsub('ROW.', "#{source}.") if options[:where]
        a.join(' AND ')
      end

      <<-SQL
        CREATE FUNCTION pg_triggers_counter_#{main_table}_#{counter_column}() RETURNS trigger
          LANGUAGE plpgsql
          AS $$
            BEGIN
              IF (TG_OP = 'INSERT') THEN
                IF (#{condition['NEW']}) THEN
                  UPDATE #{main_table} SET #{counter_column} = #{counter_column} + 1 WHERE #{where['NEW']};
                END IF;
                RETURN NEW;
              ELSIF (TG_OP = 'UPDATE') THEN
                IF (#{changed}) OR ((#{condition['OLD']}) <> (#{condition['NEW']})) THEN
                  IF (#{condition['OLD']}) THEN
                    UPDATE #{main_table} SET #{counter_column} = #{counter_column} - 1 WHERE #{where['OLD']};
                  END IF;
                  IF (#{condition['NEW']}) THEN
                    UPDATE #{main_table} SET #{counter_column} = #{counter_column} + 1 WHERE #{where['NEW']};
                  END IF;
                ELSE

                END IF;
                RETURN NEW;
              ELSIF (TG_OP = 'DELETE') THEN
                IF (#{condition['OLD']}) THEN
                  UPDATE #{main_table} SET #{counter_column} = #{counter_column} - 1 WHERE #{where['OLD']};
                END IF;
                RETURN OLD;
              END IF;
            END;
          $$;

        CREATE TRIGGER pg_triggers_counter_#{main_table}_#{counter_column}
        AFTER INSERT OR UPDATE OR DELETE ON #{counted_table}
        FOR EACH ROW EXECUTE PROCEDURE pg_triggers_counter_#{main_table}_#{counter_column}();
      SQL
    end

    def create_audit_table
      <<-SQL
        CREATE TABLE audit_table(
          id bigserial PRIMARY KEY,
          table_name text NOT NULL,
          changed_at timestamptz NOT NULL DEFAULT now(),
          changes json NOT NULL
        );
      SQL
    end

    def audit_table(table_name, options = {})
      incl = options[:include].map{|a| "'#{a}'"}.join(', ') if options[:include]
      ignore = options[:ignore].map{|a| "'#{a}'"}.join(', ') if options[:ignore]

      <<-SQL
        CREATE OR REPLACE FUNCTION pg_triggers_audit_#{table_name}() RETURNS TRIGGER
        AS $body$
          DECLARE
            changed_keys text[];
            changes json;
          BEGIN
            IF (TG_OP = 'UPDATE') THEN
              SELECT array_agg(o.key) INTO changed_keys
              FROM json_each(row_to_json(OLD)) o
              JOIN json_each(row_to_json(NEW)) n ON o.key = n.key
              WHERE o.value::text <> n.value::text;

              IF NOT (ARRAY[#{ignore}]::text[] @> changed_keys) THEN
                SELECT ('{' || string_agg('"' || key || '":' || value, ',') || '}')::json INTO changes
                FROM json_each(row_to_json(OLD))
                WHERE (
                  key = ANY(changed_keys)
                  #{"AND key NOT IN (#{ignore})" if ignore}
                )
                #{"OR key IN (#{incl})" if incl};

                INSERT INTO audit_table(table_name, changes) VALUES (TG_TABLE_NAME::TEXT, changes);
              END IF;

              RETURN OLD;
            ELSIF (TG_OP = 'DELETE') THEN
              INSERT INTO audit_table(table_name, changes) VALUES (TG_TABLE_NAME::TEXT, row_to_json(OLD));
              RETURN OLD;
            END IF;
          END
        $body$
        LANGUAGE plpgsql;

        DROP TRIGGER IF EXISTS pg_triggers_audit_#{table_name} ON #{table_name};

        CREATE TRIGGER pg_triggers_audit_#{table_name}
        AFTER UPDATE OR DELETE ON #{table_name}
        FOR EACH ROW EXECUTE PROCEDURE pg_triggers_audit_#{table_name}();
      SQL
    end
  end
end
