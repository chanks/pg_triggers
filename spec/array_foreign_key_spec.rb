require 'spec_helper'

describe PgTriggers, 'array_foreign_key' do
  before do
    DB.drop_table?(:referencing_table)
    DB.drop_table?(:referenced_table)

    DB.create_table :referencing_table do
      column :prefix_column, 'integer'
      column :referencing_column, 'integer[]'
    end

    DB.create_table :referenced_table do
      column :prefix_column, 'integer', null: false
      column :referenced_column, 'integer', null: false
    end

    DB[:referenced_table].insert(prefix_column: 1, referenced_column: 1)
    DB[:referenced_table].insert(prefix_column: 1, referenced_column: 2)
    DB[:referenced_table].insert(prefix_column: 1, referenced_column: 3)
  end

  describe "when the foreign key has no prefix" do
    before do
      DB.run(
        PgTriggers.array_foreign_key(
          referencing_table: :referencing_table,
          referencing_key:   :referencing_column,
          referenced_table:  :referenced_table,
          referenced_key:    :referenced_column,
        )
      )
    end

    describe "insertions on the referencing table" do
      it "should succeed if the array is empty" do
        DB[:referencing_table].insert(referencing_column: '{}')
      end

      it "should succeed if the array is null" do
        DB[:referencing_table].insert(referencing_column: nil)
      end

      it "should fail if the array has more than one dimension" do
        proc{DB[:referencing_table].insert(referencing_column: '{{1},{2},{3}}')}.
          should raise_error(Sequel::DatabaseError, /Foreign key array referencing_column has more than 1 dimension/)
      end

      it "should fail if the array has duplicate entries" do
        proc{DB[:referencing_table].insert(referencing_column: '{2,1,2}')}.
          should raise_error(Sequel::DatabaseError, /Duplicate entry in foreign key array referencing_column/)
      end

      it "should succeed if all the array values exist in the referenced table" do
        DB[:referencing_table].insert(referencing_column: '{1,2,3}')
      end

      it "should fail if any of the array values do not exist in the referenced table" do
        proc{DB[:referencing_table].insert(referencing_column: '{1,2,4}')}.
          should raise_error(Sequel::DatabaseError, /Entry in foreign key array \(referencing_column\) not in referenced column \(referenced_column\)/)
      end
    end

    describe "updates on the referencing table" do
      before do
        DB[:referencing_table].insert(prefix_column: 1, referencing_column: '{1,2,3}')
      end

      it "should succeed if the array is set to empty" do
        DB[:referencing_table].where(prefix_column: 1).update(referencing_column: '{}').should be 1
      end

      it "should succeed if the array is set to null" do
        DB[:referencing_table].where(prefix_column: 1).update(referencing_column: nil).should be 1
      end

      it "should fail if the array has more than one dimension" do
        proc{DB[:referencing_table].where(prefix_column: 1).update(referencing_column: '{{1},{2},{3}}')}.
          should raise_error(Sequel::DatabaseError, /Foreign key array referencing_column has more than 1 dimension/)
      end

      it "should fail if the array has duplicate entries" do
        proc{DB[:referencing_table].where(prefix_column: 1).update(referencing_column: '{2,1,2}')}.
          should raise_error(Sequel::DatabaseError, /Duplicate entry in foreign key array referencing_column/)
      end

      it "should succeed if all the array values exist in the referenced table" do
        DB[:referencing_table].where(prefix_column: 1).update(referencing_column: '{1,2,3}').should be 1
      end

      it "should succeed if the array values are unchanged" do
        DB[:referencing_table].where(prefix_column: 1).update(referencing_column: '{1,2,3}').should be 1
      end

      it "should fail if any of the array values do not exist in the referenced table" do
        proc{DB[:referencing_table].where(prefix_column: 1).update(referencing_column: '{1,2,4}')}.
          should raise_error(Sequel::DatabaseError, /Entry in foreign key array \(referencing_column\) not in referenced column \(referenced_column\)/)
      end
    end

    describe "deletes on the referencing table" do
      before do
        DB[:referencing_table].insert(prefix_column: 1, referencing_column: '{1,2,3}')
      end

      it "should succeed" do
        DB[:referencing_table].where(prefix_column: 1).delete.should be 1
      end
    end

    describe "on the referenced table" do
      before do
        DB[:referencing_table].insert(prefix_column: 1, referencing_column: '{1,2,3}')
      end

      describe "inserts" do
        it "should succeed" do
          DB[:referenced_table].insert(prefix_column: 1, referenced_column: 1)
        end
      end

      describe "updates" do
        it "should succeed if the referenced column is unchanged" do
          DB[:referenced_table].where(prefix_column: 1, referenced_column: 1).update(referenced_column: 1).should be 1
        end

        it "should succeed if the given record is not still referenced" do
          DB[:referencing_table].where(prefix_column: 1).update(referencing_column: '{2,3}').should be 1
          DB[:referenced_table].where(prefix_column: 1, referenced_column: 1).update(referenced_column: 4).should be 1
        end

        it "should fail if the referenced column is still referenced" do
          proc{DB[:referenced_table].where(prefix_column: 1, referenced_column: 1).update(referenced_column: 4)}.
            should raise_error(Sequel::DatabaseError, /Entry in referenced column \(referenced_column\) still in foreign key array \(referencing_column\)/)
        end
      end

      describe "deletes" do
        it "should succeed if the given record is not still referenced" do
          DB[:referencing_table].where(prefix_column: 1).update(referencing_column: '{2,3}').should be 1
          DB[:referenced_table].where(prefix_column: 1, referenced_column: 1).delete.should be 1
        end

        it "should fail if the referenced column is still referenced" do
          proc{DB[:referenced_table].where(prefix_column: 1, referenced_column: 1).delete}.
            should raise_error(Sequel::DatabaseError, /Entry in referenced column \(referenced_column\) still in foreign key array \(referencing_column\)/)
        end
      end
    end
  end

  describe "when the foreign key has a prefix" do
    before do
      DB.run(
        PgTriggers.array_foreign_key(
          referencing_table: :referencing_table,
          referencing_key:   [:prefix_column, :referencing_column],
          referenced_table:  :referenced_table,
          referenced_key:    [:prefix_column, :referenced_column],
        )
      )
    end

    describe "insertions on the referencing table" do
      it "should succeed if the array is empty" do
        # prefix_column value shouldn't matter
        DB[:referencing_table].insert(prefix_column: 78, referencing_column: '{}')
      end

      it "should succeed if the array is null" do
        DB[:referencing_table].insert(prefix_column: 78, referencing_column: nil)
      end

      it "should succeed if the prefix column is null" do
        DB[:referencing_table].insert(prefix_column: nil, referencing_column: '{45,46,47}')
      end

      it "should fail if the array has more than one dimension" do
        proc{DB[:referencing_table].insert(prefix_column: 1, referencing_column: '{{1},{2},{3}}')}.
          should raise_error(Sequel::DatabaseError, /Foreign key array referencing_column has more than 1 dimension/)
      end

      it "should fail if the array has duplicate entries" do
        proc{DB[:referencing_table].insert(prefix_column: 1, referencing_column: '{2,1,2}')}.
          should raise_error(Sequel::DatabaseError, /Duplicate entry in foreign key array referencing_column/)
      end

      it "should succeed if all the array values exist in the referenced table" do
        DB[:referencing_table].insert(prefix_column: 1, referencing_column: '{1,2,3}')
      end

      it "should fail if any of the array values do not exist in the referenced table" do
        proc{DB[:referencing_table].insert(prefix_column: 1, referencing_column: '{1,2,4}')}.
          should raise_error(Sequel::DatabaseError, /Entry in foreign key array \(prefix_column, referencing_column\) not in referenced column \(prefix_column, referenced_column\)/)
      end

      it "should fail if the prefix columns do not exist in the referenced table" do
        proc{DB[:referencing_table].insert(prefix_column: 2, referencing_column: '{1,2,4}')}.
          should raise_error(Sequel::DatabaseError, /Entry in foreign key array \(prefix_column, referencing_column\) not in referenced column \(prefix_column, referenced_column\)/)
      end
    end

    describe "updates on the referencing table" do
      before do
        DB[:referencing_table].insert(prefix_column: 1, referencing_column: '{1,2,3}')
      end

      it "should succeed if the array is set to empty" do
        DB[:referencing_table].where(prefix_column: 1).update(referencing_column: '{}').should be 1
      end

      it "should succeed if the array is set to null" do
        DB[:referencing_table].where(prefix_column: 1).update(referencing_column: nil).should be 1
      end

      it "should succeed if the prefix column is set to null" do
        DB[:referencing_table].where(prefix_column: 1).update(prefix_column: nil).should be 1
      end

      it "should fail if the array has more than one dimension" do
        proc{DB[:referencing_table].where(prefix_column: 1).update(referencing_column: '{{1},{2},{3}}')}.
          should raise_error(Sequel::DatabaseError, /Foreign key array referencing_column has more than 1 dimension/)
      end

      it "should fail if the array has duplicate entries" do
        proc{DB[:referencing_table].where(prefix_column: 1).update(referencing_column: '{2,1,2}')}.
          should raise_error(Sequel::DatabaseError, /Duplicate entry in foreign key array referencing_column/)
      end

      it "should succeed if all the array values exist in the referenced table" do
        DB[:referencing_table].where(prefix_column: 1).update(referencing_column: '{1,2,3}').should be 1
      end

      it "should succeed if the prefixed coliumn changes but the referenced values still exist in the referenced table" do
        DB[:referenced_table].insert(prefix_column: 2, referenced_column: 1)
        DB[:referenced_table].insert(prefix_column: 2, referenced_column: 2)
        DB[:referenced_table].insert(prefix_column: 2, referenced_column: 3)
        DB[:referencing_table].where(prefix_column: 1).update(prefix_column: 2).should be 1
      end

      it "should succeed if the array values are unchanged" do
        DB[:referencing_table].where(prefix_column: 1).update(referencing_column: '{1,2,3}').should be 1
      end

      it "should succeed if the prefix value is unchanged" do
        DB[:referencing_table].where(prefix_column: 1).update(prefix_column: 1).should be 1
      end

      it "should fail if any of the array values do not exist in the referenced table" do
        proc{DB[:referencing_table].where(prefix_column: 1).update(referencing_column: '{1,2,4}')}.
          should raise_error(Sequel::DatabaseError, /Entry in foreign key array \(prefix_column, referencing_column\) not in referenced column \(prefix_column, referenced_column\)/)
      end

      it "should fail if the prefix column for the given array values doesn't exist in the referenced table" do
        DB[:referenced_table].insert(prefix_column: 2, referenced_column: 1)
        DB[:referenced_table].insert(prefix_column: 2, referenced_column: 2)
        proc{DB[:referencing_table].where(prefix_column: 1).update(prefix_column: 2)}.
          should raise_error(Sequel::DatabaseError, /Entry in foreign key array \(prefix_column, referencing_column\) not in referenced column \(prefix_column, referenced_column\)/)
      end
    end

    describe "deletes on the referencing table" do
      before do
        DB[:referencing_table].insert(prefix_column: 1, referencing_column: '{1,2,3}')
      end

      it "should succeed" do
        DB[:referencing_table].where(prefix_column: 1).delete.should be 1
      end
    end

    describe "on the referenced table" do
      before do
        DB[:referencing_table].insert(prefix_column: 1, referencing_column: '{1,2,3}')
      end

      describe "inserts" do
        it "should succeed" do
          DB[:referenced_table].insert(prefix_column: 1, referenced_column: 4)
        end
      end

      describe "updates" do
        it "should succeed if the referenced column is unchanged" do
          DB[:referenced_table].where(prefix_column: 1, referenced_column: 1).update(referenced_column: 1).should be 1
        end

        it "should succeed if the prefix column is unchanged" do
          DB[:referenced_table].where(prefix_column: 1, referenced_column: 1).update(prefix_column: 1).should be 1
        end

        it "should succeed if the given record is not still referenced when the referenced column is updated" do
          DB[:referencing_table].where(prefix_column: 1).update(referencing_column: '{2,3}').should be 1
          DB[:referenced_table].where(prefix_column: 1, referenced_column: 1).update(referenced_column: 4).should be 1
        end

        it "should succeed if the given record is not still referenced when the prefix column is updated" do
          DB[:referenced_table].insert(prefix_column: 2, referenced_column: 1)
          DB[:referenced_table].insert(prefix_column: 2, referenced_column: 2)
          DB[:referenced_table].insert(prefix_column: 2, referenced_column: 3)

          DB[:referencing_table].where(prefix_column: 1).update(prefix_column: 2).should be 1
          DB[:referenced_table].where(prefix_column: 1, referenced_column: 1).update(prefix_column: 8).should be 1
        end

        it "should fail if the referenced column is still referenced when it is changed" do
          proc{DB[:referenced_table].where(prefix_column: 1, referenced_column: 1).update(referenced_column: 4)}.
            should raise_error(Sequel::DatabaseError, /Entry in referenced column \(prefix_column, referenced_column\) still in foreign key array \(prefix_column, referencing_column\)/)
        end

        it "should fail if the prefix column is still referenced when it is changed" do
          proc{DB[:referenced_table].where(prefix_column: 1, referenced_column: 1).update(prefix_column: 4)}.
            should raise_error(Sequel::DatabaseError, /Entry in referenced column \(prefix_column, referenced_column\) still in foreign key array \(prefix_column, referencing_column\)/)
        end
      end

      describe "deletes" do
        it "should succeed if the given column is not still referenced" do
          DB[:referencing_table].where(prefix_column: 1).update(referencing_column: '{2,3}').should be 1
          DB[:referenced_table].where(prefix_column: 1, referenced_column: 1).delete.should be 1
        end

        it "should succeed if the given prefix is not still referenced" do
          DB[:referenced_table].insert(prefix_column: 2, referenced_column: 1)
          DB[:referenced_table].insert(prefix_column: 2, referenced_column: 2)
          DB[:referenced_table].insert(prefix_column: 2, referenced_column: 3)

          DB[:referencing_table].where(prefix_column: 1).update(prefix_column: 2).should be 1
          DB[:referenced_table].where(prefix_column: 1, referenced_column: 1).delete.should be 1
        end

        it "should fail if the given column is still referenced" do
          proc{DB[:referenced_table].where(prefix_column: 1, referenced_column: 1).delete}.
            should raise_error(Sequel::DatabaseError, /Entry in referenced column \(prefix_column, referenced_column\) still in foreign key array \(prefix_column, referencing_column\)/)
        end
      end
    end
  end
end
