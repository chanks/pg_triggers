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

    def array_foreign_key(referencing_table:, referencing_key:, referenced_table:, referenced_key:)
      referencing_key = Array(referencing_key)
      referenced_key  = Array(referenced_key)

      raise "Mismatched referencing and referenced keys!" unless referencing_key.length == referenced_key.length

      referencing_function_name = "pt_afka_#{referencing_table}_#{referencing_key.join('_')}".slice(0...63)
      referenced_function_name  = "pt_afkb_#{referencing_table}_#{referencing_key.join('_')}".slice(0...63)

      *_referencing_prefix_columns, referencing_column = referencing_key
      *_referenced_prefix_columns,  referenced_column  = referenced_key

      referencing_column_display = %("#{referencing_table}"."#{referencing_column}")

      referencing_key_display = referencing_key.map{|k| %("#{referencing_table}"."#{k}")}
      referenced_key_display  = referenced_key. map{|k| %("#{referenced_table}"."#{k}")}
      referencing_key_display = referencing_key_display.length > 1 ? "(#{referencing_key_display.join(", ")})" : referencing_key_display.first
      referenced_key_display  = referenced_key_display.length  > 1 ? "(#{referenced_key_display.join(", ")})"  : referenced_key_display.first

      referencing_key_unchanged = referencing_key.map{|k| "(NEW.#{k} IS NOT DISTINCT FROM OLD.#{k})"}.join(" AND ")
      referenced_key_unchanged  = referenced_key. map{|k| "(NEW.#{k} IS NOT DISTINCT FROM OLD.#{k})"}.join(" AND ")

      referencing_key_missing = referencing_key.map{|k| "(NEW.#{k} IS NULL)"}.join(" OR ")
      referenced_key_missing  = referenced_key.map {|k| "(OLD.#{k} IS NULL)"}.join(" OR ")

      referencing_change_query = referencing_key.zip(referenced_key).map do |c1, c2|
        if c1 == referencing_column
          "#{c2} = ANY(NEW.#{c1})"
        else
          "#{c2} = NEW.#{c1}"
        end
      end.join(" AND ")

      referenced_change_query = referenced_key.zip(referencing_key).map do |c1, c2|
        if c1 == referenced_column
          "#{c2} @> ARRAY[OLD.#{c1}]"
        else
          "#{c2} = OLD.#{c1}"
        end
      end.join(" AND ")

      <<-SQL
        CREATE OR REPLACE FUNCTION #{referencing_function_name}() RETURNS trigger
          LANGUAGE plpgsql
          AS $$
            DECLARE
              arr #{referencing_table}.#{referencing_column}%TYPE;
              temp_count1 int;
              temp_count2 int;
            BEGIN
              IF ((TG_OP = 'UPDATE' AND #{referencing_key_unchanged}) OR (#{referencing_key_missing})) THEN
                RETURN NULL;
              END IF;

              arr := NEW.#{referencing_column};

              -- Check validity of the new array, if necessary.
              IF (TG_OP = 'INSERT' OR NEW.#{referencing_column} IS DISTINCT FROM OLD.#{referencing_column}) THEN
                temp_count1 := array_ndims(arr);
                IF arr IS NULL OR temp_count1 IS NULL THEN
                  RETURN NULL;
                END IF;

                IF temp_count1 IS DISTINCT FROM 1 THEN
                  RAISE EXCEPTION 'Foreign key array column #{referencing_column_display} has more than 1 dimension: %, dimensions: %', arr, temp_count1;
                END IF;

                SELECT count(*) INTO temp_count1 FROM unnest(arr);
                SELECT count(*) INTO temp_count2 FROM (SELECT DISTINCT * FROM unnest(arr)) AS t;
                IF temp_count1 IS DISTINCT FROM temp_count2 THEN
                  RAISE EXCEPTION 'Duplicate entry in foreign key array column #{referencing_column_display}: %', arr;
                END IF;
              END IF;

              SELECT COUNT(*) INTO temp_count1 FROM #{referenced_table} WHERE (#{referencing_change_query});
              temp_count2 := array_length(arr, 1);
              IF temp_count1 IS DISTINCT FROM temp_count2 THEN
                RAISE EXCEPTION 'Entry in foreign key array #{referencing_key_display} not found in #{referenced_key_display}: %', arr;
              END IF;

              RETURN NULL;
            END;
          $$;

        DROP TRIGGER IF EXISTS #{referencing_function_name} ON #{referencing_table};

        CREATE TRIGGER #{referencing_function_name}
        AFTER INSERT OR UPDATE ON #{referencing_table}
        FOR EACH ROW EXECUTE PROCEDURE #{referencing_function_name}();

        CREATE OR REPLACE FUNCTION #{referenced_function_name}() RETURNS trigger
          LANGUAGE plpgsql
          AS $$
            BEGIN
              IF ((TG_OP = 'UPDATE' AND #{referenced_key_unchanged}) OR (#{referenced_key_missing})) THEN
                RETURN NULL;
              END IF;

              PERFORM true FROM #{referencing_table} WHERE (#{referenced_change_query});
              IF FOUND THEN
                RAISE EXCEPTION 'Entry in referenced column #{referenced_key_display} still in foreign key array #{referencing_key_display}: %', OLD.#{referenced_column};
              END IF;

              RETURN NULL;
            END;
          $$;

        DROP TRIGGER IF EXISTS #{referenced_function_name} ON #{referenced_table};

        CREATE TRIGGER #{referenced_function_name}
        AFTER UPDATE OR DELETE ON #{referenced_table}
        FOR EACH ROW EXECUTE PROCEDURE #{referenced_function_name}();
      SQL
    end
  end
end
