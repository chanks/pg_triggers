require "pg_triggers/version"

module PgTriggers
  class << self
    def counter_cache(
      counting_table:,
      counting_column:,
      counted_table:,
      relationship:,
      increment: 1,
      name: nil,
      where: nil
    )
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

    def sum_cache(
      summing_table:,
      summing_column:,
      summed_table:,
      summed_column:,
      relationship:,
      multiplier: 1,
      name: nil,
      where: nil
    )

      relationship_condition = proc { |source| relationship.map{|k, v| "#{k} = #{source}.#{v}"}.join(' AND ') }

      columns = relationship.values
      changed = columns.map{|c| "((OLD.#{c} <> NEW.#{c}) OR ((OLD.#{c} IS NULL) <> (NEW.#{c} IS NULL)))"}.join(' OR ')
      name  ||= "pt_sc_#{summing_table}_#{summing_column}"

      row_condition = proc do |source|
        a = []
        a << "(#{columns.map{|c| "#{source}.#{c} IS NOT NULL"}.join(' AND ')})"
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
                  UPDATE #{summing_table} SET #{summing_column} = #{summing_column} + (NEW.#{summed_column} * #{multiplier}) WHERE #{relationship_condition['NEW']};
                END IF;
                RETURN NEW;
              ELSIF (TG_OP = 'UPDATE') THEN
                IF (#{changed}) OR ((#{row_condition['OLD']}) <> (#{row_condition['NEW']})) THEN
                  IF (#{row_condition['OLD']}) THEN
                    UPDATE #{summing_table} SET #{summing_column} = #{summing_column} - (OLD.#{summed_column} * #{multiplier}) WHERE #{relationship_condition['OLD']};
                  END IF;
                  IF (#{row_condition['NEW']}) THEN
                    UPDATE #{summing_table} SET #{summing_column} = #{summing_column} + (NEW.#{summed_column} * #{multiplier}) WHERE #{relationship_condition['NEW']};
                  END IF;
                ELSIF (OLD.#{summed_column} <> NEW.#{summed_column}) THEN
                  UPDATE #{summing_table} SET #{summing_column} = #{summing_column} + ((NEW.#{summed_column} - OLD.#{summed_column}) * #{multiplier}) WHERE #{relationship_condition['NEW']};
                END IF;
                RETURN NEW;
              ELSIF (TG_OP = 'DELETE') THEN
                IF (#{row_condition['OLD']}) THEN
                  UPDATE #{summing_table} SET #{summing_column} = #{summing_column} - (OLD.#{summed_column} * #{multiplier}) WHERE #{relationship_condition['OLD']};
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

    def updated_at(table:, column:)
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
  end
end
