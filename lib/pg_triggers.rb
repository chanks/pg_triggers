require "pg_triggers/version"

module PgTriggers
  class << self
    def counter_cache(counting_table:, counting_column:, counted_table:, relationship:, increment: 1, name: nil, where: nil)
      relationship_condition = proc { |source| relationship.map{|k, v| "#{k} = #{source}.#{v}"}.join(' AND ') }

      columns = relationship.values
      changed = columns.map{|c| "((OLD.#{c} <> NEW.#{c}) OR ((OLD.#{c} IS NULL) <> (NEW.#{c} IS NULL)))"}.join(' OR ')
      name  ||= "pt_cc_#{counting_table}_#{counting_column}"

      row_condition = proc do |source|
        a = []
        a << "(#{columns.map{|c| "(#{source}.#{c} IS NOT NULL)"}.join(' AND ')})"
        a << "(#{where.gsub('ROW.', "#{source}.")})" if where
        a.join(' AND ')
      end

      <<-SQL
        CREATE OR REPLACE FUNCTION #{name}() RETURNS trigger
          LANGUAGE plpgsql
          AS $$
            BEGIN
              IF (TG_OP = 'INSERT') THEN
                IF (#{row_condition['NEW']}) THEN
                  UPDATE #{counting_table} SET #{counting_column} = #{counting_column} + #{increment} WHERE #{relationship_condition['NEW']};
                END IF;
                RETURN NEW;
              ELSIF (TG_OP = 'UPDATE') THEN
                IF (#{changed}) OR ((#{row_condition['OLD']}) <> (#{row_condition['NEW']})) THEN
                  IF (#{row_condition['OLD']}) THEN
                    UPDATE #{counting_table} SET #{counting_column} = #{counting_column} - #{increment} WHERE #{relationship_condition['OLD']};
                  END IF;
                  IF (#{row_condition['NEW']}) THEN
                    UPDATE #{counting_table} SET #{counting_column} = #{counting_column} + #{increment} WHERE #{relationship_condition['NEW']};
                  END IF;
                END IF;
                RETURN NEW;
              ELSIF (TG_OP = 'DELETE') THEN
                IF (#{row_condition['OLD']}) THEN
                  UPDATE #{counting_table} SET #{counting_column} = #{counting_column} - #{increment} WHERE #{relationship_condition['OLD']};
                END IF;
                RETURN OLD;
              END IF;
            END;
          $$;

        DROP TRIGGER IF EXISTS #{name} ON #{counted_table};

        CREATE TRIGGER #{name}
        AFTER INSERT OR UPDATE OR DELETE ON #{counted_table}
        FOR EACH ROW EXECUTE PROCEDURE #{name}();
      SQL
    end

    def sum_cache(main_table, sum_column, summed_table, summed_column, relationship, options = {})
      where      = proc { |source| relationship.map{|k, v| "#{k} = #{source}.#{v}"}.join(' AND ') }
      columns    = relationship.values
      changed    = columns.map{|c| "((OLD.#{c} <> NEW.#{c}) OR ((OLD.#{c} IS NULL) <> (NEW.#{c} IS NULL)))"}.join(' OR ')
      multiplier = (options[:multiplier] || 1).to_i
      name       = options[:name] || "pt_sc_#{main_table}_#{sum_column}"

      condition = proc do |source|
        a = []
        a << "(#{columns.map{|c| "#{source}.#{c} IS NOT NULL"}.join(' AND ')})"
        a << "(#{options[:where].gsub('ROW.', "#{source}.")})" if options[:where]
        a.join(' AND ')
      end

      <<-SQL
        CREATE OR REPLACE FUNCTION #{name}() RETURNS trigger
          LANGUAGE plpgsql
          AS $$
            BEGIN
              IF (TG_OP = 'INSERT') THEN
                IF (#{condition['NEW']}) THEN
                  UPDATE #{main_table} SET #{sum_column} = #{sum_column} + (NEW.#{summed_column} * #{multiplier}) WHERE #{where['NEW']};
                END IF;
                RETURN NEW;
              ELSIF (TG_OP = 'UPDATE') THEN
                IF (#{changed}) OR ((#{condition['OLD']}) <> (#{condition['NEW']})) THEN
                  IF (#{condition['OLD']}) THEN
                    UPDATE #{main_table} SET #{sum_column} = #{sum_column} - (OLD.#{summed_column} * #{multiplier}) WHERE #{where['OLD']};
                  END IF;
                  IF (#{condition['NEW']}) THEN
                    UPDATE #{main_table} SET #{sum_column} = #{sum_column} + (NEW.#{summed_column} * #{multiplier}) WHERE #{where['NEW']};
                  END IF;
                ELSIF (OLD.#{summed_column} <> NEW.#{summed_column}) THEN
                  UPDATE #{main_table} SET #{sum_column} = #{sum_column} + ((NEW.#{summed_column} - OLD.#{summed_column}) * #{multiplier}) WHERE #{where['NEW']};
                END IF;
                RETURN NEW;
              ELSIF (TG_OP = 'DELETE') THEN
                IF (#{condition['OLD']}) THEN
                  UPDATE #{main_table} SET #{sum_column} = #{sum_column} - (OLD.#{summed_column} * #{multiplier}) WHERE #{where['OLD']};
                END IF;
                RETURN OLD;
              END IF;
            END;
          $$;

        DROP TRIGGER IF EXISTS #{name} ON #{summed_table};

        CREATE TRIGGER #{name}
        AFTER INSERT OR UPDATE OR DELETE ON #{summed_table}
        FOR EACH ROW EXECUTE PROCEDURE #{name}();
      SQL
    end

    def updated_at(table, column)
      <<-SQL
        CREATE OR REPLACE FUNCTION pt_u_#{table}_#{column}() RETURNS trigger
          LANGUAGE plpgsql
          AS $$
            BEGIN
              IF (TG_OP = 'INSERT') THEN
                IF NEW.updated_at IS NULL THEN
                  NEW.updated_at := CURRENT_TIMESTAMP;
                END IF;
              ELSIF (TG_OP = 'UPDATE') THEN
                IF NEW.updated_at = OLD.updated_at THEN
                  NEW.updated_at := CURRENT_TIMESTAMP;
                END IF;
              END IF;

              RETURN NEW;
            END;
          $$;

        DROP TRIGGER IF EXISTS pt_u_#{table}_#{column} ON #{table};

        CREATE TRIGGER pt_u_#{table}_#{column}
        BEFORE INSERT OR UPDATE ON #{table}
        FOR EACH ROW EXECUTE PROCEDURE pt_u_#{table}_#{column}();
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
        CREATE OR REPLACE FUNCTION pt_a_#{table_name}() RETURNS TRIGGER
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

        DROP TRIGGER IF EXISTS pt_a_#{table_name} ON #{table_name};

        CREATE TRIGGER pt_a_#{table_name}
        AFTER UPDATE OR DELETE ON #{table_name}
        FOR EACH ROW EXECUTE PROCEDURE pt_a_#{table_name}();
      SQL
    end
  end
end
