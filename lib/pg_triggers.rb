# frozen_string_literal: true

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

    def conditional_foreign_key(
      parent_table:,
      child_table:,
      relationship:,
      parent_trigger_name: nil,
      child_trigger_name: nil
    )

      parent_columns = relationship.keys
      child_columns  = relationship.values

      parent_has_key_values     = parent_columns.map{|c| "(OLD.#{c} IS NOT NULL)"}.join(" AND ")
      parent_key_values_changed = parent_columns.map{|c| "(OLD.#{c} <> NEW.#{c}) OR ((OLD.#{c} IS NULL) <> (NEW.#{c} IS NULL))"}.join(" OR ")

      child_has_key_values     = child_columns.map {|c| "(NEW.#{c} IS NOT NULL)"}.join(" AND ")
      child_key_values_changed = child_columns.map{|c| "(OLD.#{c} <> NEW.#{c}) OR ((OLD.#{c} IS NULL) <> (NEW.#{c} IS NULL))"}.join(" OR ")

      parent_trigger_name ||= "pt_cfk_#{parent_table}_#{parent_columns.join('_')}"
      child_trigger_name  ||= "pt_cfk_#{child_table }_#{child_columns. join('_')}"

      parent_condition = proc { |source| relationship.map{|k, v| "#{k} = #{source}.#{v}"}.join(' AND ') }
      child_condition  = proc { |source| relationship.map{|k, v| "#{v} = #{source}.#{k}"}.join(' AND ') }

      <<-SQL
        CREATE OR REPLACE FUNCTION #{parent_trigger_name}() RETURNS trigger
          LANGUAGE plpgsql
          AS $$
            DECLARE
              result boolean;
            BEGIN
              IF (#{parent_has_key_values}) AND (TG_OP = 'DELETE' OR (#{parent_key_values_changed})) THEN
                SELECT true INTO result FROM #{child_table} WHERE #{child_condition['OLD']} FOR SHARE OF #{child_table};

                IF FOUND THEN
                  RAISE EXCEPTION '% in #{parent_table} violates foreign key constraint "#{parent_trigger_name}"', lower(TG_OP);
                END IF;
              END IF;

              IF (TG_OP = 'UPDATE') THEN
                RETURN NEW;
              ELSE
                RETURN OLD;
              END IF;
            END;
          $$;

        DROP TRIGGER IF EXISTS #{parent_trigger_name} ON #{parent_table};

        CREATE TRIGGER #{parent_trigger_name}
        AFTER UPDATE OR DELETE ON #{parent_table}
        FOR EACH ROW EXECUTE PROCEDURE #{parent_trigger_name}();

        CREATE OR REPLACE FUNCTION #{child_trigger_name}() RETURNS trigger
          LANGUAGE plpgsql
          AS $$
            DECLARE
              result boolean;
            BEGIN
              IF (#{child_has_key_values}) AND (TG_OP = 'INSERT' OR (#{child_key_values_changed})) THEN
                SELECT true INTO result FROM #{parent_table} WHERE #{parent_condition['NEW']} FOR KEY SHARE OF #{parent_table};

                IF NOT FOUND THEN
                  RAISE EXCEPTION '% in #{child_table} violates foreign key constraint "#{child_trigger_name}"', lower(TG_OP);
                END IF;
              END IF;

              RETURN NEW;
            END;
          $$;

        DROP TRIGGER IF EXISTS #{child_trigger_name} ON #{child_table};

        CREATE TRIGGER #{child_trigger_name}
        AFTER INSERT OR UPDATE ON #{child_table}
        FOR EACH ROW EXECUTE PROCEDURE #{child_trigger_name}();
      SQL
    end
  end
end
