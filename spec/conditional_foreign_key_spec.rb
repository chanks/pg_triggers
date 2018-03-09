require 'spec_helper'

describe PgTriggers, 'conditional_foreign_key' do
  before do
    DB.drop_table?(:parents)
    DB.drop_table?(:children)

    DB.create_table :parents do
      primary_key :id1
      integer     :id2

      integer :other_column

      unique [:id1, :id2]
    end

    DB.create_table :children do
      primary_key :id

      integer :parent_id1
      integer :parent_id2

      integer :other_column
    end
  end

  describe "without extra options" do
    before do
      DB.run \
        PgTriggers.conditional_foreign_key(
          parent_table: :parents,
          child_table: :children,
          relationship: {id1: :parent_id1, id2: :parent_id2},
        )

      DB[:parents].insert(id1: 1, id2: 3)
      DB[:parents].insert(id1: 2, id2: 4)
      DB[:children].insert(parent_id1: 1, parent_id2: 3)
    end

    describe "when modifying a parent" do
      describe "with an update" do
        it "should not throw an error when updating a parent that has no referring children" do
          DB[:parents].where(id1: 2).update(id1: 3).should == 1
        end

        it "should not throw an error when updating a parent that has only a partial key" do
          DB[:parents].where(id1: 2).update(id2: nil).should == 1
          DB[:parents].where(id1: 2).update(other_column: 17).should == 1
          DB[:parents].where(id1: 2).update(id2: 2).should == 1
        end

        it "should not throw an error when updating non-key columns of a parent that a child is referring to" do
          DB[:parents].where(id1: 1, id2: 3).update(other_column: 7).should == 1
        end

        it "should throw an error when updating the key of a parent that a child is pointing to" do
          key = [:id1, :id2].sample
          # TODO: Figure out what to raise to make these ForeignKeyConstraint violations.
          proc{DB[:parents].where(id1: 1).update(key => 7)}.should raise_error(Sequel::DatabaseError, /update in parents violates foreign key constraint/)
        end

        it "should throw an error when setting null the key of a parent that a child is pointing to" do
          proc{DB[:parents].where(id1: 1).update(id2: nil)}.should raise_error(Sequel::DatabaseError, /update in parents violates foreign key constraint/)
        end
      end

      describe "with an delete" do
        it "should not throw an error when deleting a parent that has no referring children" do
          DB[:parents].where(id1: 2).delete.should == 1
        end

        it "should not throw an error when deleting a parent that has an incomplete key" do
          DB[:parents].where(id1: 2).update(id2: nil).should == 1
          DB[:parents].where(id1: 2).delete.should == 1
        end

        it "should throw an error when deleting a parent that a child is pointing to" do
          proc{DB[:parents].where(id1: 1).delete}.should raise_error(Sequel::DatabaseError, /delete in parents violates foreign key constraint/)
        end
      end
    end

    describe "when modifying a child" do
      describe "with an insert" do
        it "should not throw an error when the specified parent exists"

        it "should throw an error when the specified parent doesn't exist" do
          proc{DB[:children].insert(id: 2, parent_id1: 1, parent_id2: 4)}.should raise_error(Sequel::DatabaseError, /insert in children violates foreign key constraint/)
        end

        it "should not throw an error when the child doesn't have a complete foreign key" do
          skip "Not yet implemented"

          DB[:children].insert(id: 1, parent_id1: 1)
          DB[:children].where(id: 1).update(parent_id1: 3)
        end
      end

      describe "with an update" do
        it "should not throw an error when updating a child whose specified parent exists" do
          DB[:children].where(id: 1).update(other_column: 3).should == 1
        end

        it "should throw an error when updating a child's foreign key when the new parent doesn't exist" do
          proc{DB[:children].where(id: 1).update(parent_id2: 4)}.should raise_error(Sequel::DatabaseError, /update in children violates foreign key constraint/)
        end

        it "should not throw an error when the new child record doesn't have a complete foreign key" do
          skip "Not yet implemented"

          DB[:children].insert(id: 1, parent_id1: 1)
          DB[:children].where(id: 1).update(parent_id1: 3)
        end
      end
    end
  end

  describe "with a parent_condition argument" do
    # TODO
  end

  describe "with a child_condition argument" do
    # Practical use of this?
  end
end
