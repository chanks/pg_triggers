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
  end
end
