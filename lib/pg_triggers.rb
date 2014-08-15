require "pg_triggers/version"

module PgTriggers
  class << self
    def counter_cache(main_table, counter_column, counted_table, relationship, options = {})
      where = proc { |source| relationship.map{|k, v| "#{k} = #{source}.#{v}"}.join(' AND ') }

      <<-SQL
        CREATE FUNCTION pg_triggers_counter_#{main_table}_#{counter_column}() RETURNS trigger
          LANGUAGE plpgsql
          AS $$
            BEGIN
              IF (TG_OP = 'INSERT') THEN
                UPDATE #{main_table} SET #{counter_column} = #{counter_column} + 1 WHERE #{where['NEW']};
                RETURN NEW;
              ELSIF (TG_OP = 'DELETE') THEN
                UPDATE #{main_table} SET #{counter_column} = #{counter_column} - 1 WHERE #{where['OLD']};
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
