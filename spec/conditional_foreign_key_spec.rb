require 'spec_helper'

describe PgTriggers, 'conditional_foreign_key' do
  before do
    DB.drop_table?(:parents)
    DB.drop_table?(:children)

    DB.create_table :parents do
      integer :id1, null: false
      integer :id2, null: false
      primary_key [:id1, :id2]

      integer :number
    end

    DB.create_table :children do
      primary_key :id

      integer :parent_id1
      integer :parent_id2
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
    end

    it "should not throw an error when updating a parent that has no referring children" do
      DB[:parents].insert(id1: 1, id2: 2)
      DB[:parents].where(id1: 1, id2: 2).update(id1: 4).should == 1
    end

    it "should not throw an error when deleting a parent that has no referring children" do
      DB[:parents].insert(id1: 1, id2: 2)
      DB[:parents].where(id1: 1, id2: 2).delete.should == 1
    end

    it "should throw an error when updating the key of a parent that a child is pointing to" do
      DB[:parents].insert(id1: 1, id2: 2)
      DB[:children].insert(parent_id1: 1, parent_id2: 2)

      key = [:id1, :id2].sample

      # TODO: Figure out what to raise to make these ForeignKeyConstraint violations.
      proc{DB[:parents].where(id1: 1, id2: 2).update(key => 3)}.should raise_error(Sequel::DatabaseError, /update in parents violates foreign key constraint/)
    end

    it "should throw an error when deleting a parent that a child is pointing to" do
      DB[:parents].insert(id1: 1, id2: 2)
      DB[:children].insert(parent_id1: 1, parent_id2: 2)

      proc{DB[:parents].where(id1: 1, id2: 2).delete}.should raise_error(Sequel::DatabaseError, /delete in parents violates foreign key constraint/)
    end

    it "should not throw an error when inserting/deleting a child whose specified parent exists"

    it "should throw an error when inserting a child whose specified parent doesn't exist"

    it "should not throw an error when inserting/updating/deleting a child with an incomplete foreign key"
  end

  describe "with a parent_condition argument" do
    it "should not throw an error when deleting a parent that has no referring children"

    it "should throw an error when deleting a parent that a child is pointing to"

    it "should throw an error when updating the key of a parent that a child is pointing to"

    it "should throw an error when a parent stops having the condition that a child is pointing to"

    it "should not throw an error when inserting a child whose specified parent exists"

    it "should throw an error when inserting a child whose specified parent doesn't exist"

    it "should throw an error when inserting a child whose specified parent doesn't match the condition"

    it "should throw an error when updating a child whose newly-specified parent doesn't exist"

    it "should throw an error when updating a child whose newly-specified parent doesn't match the condition"

    it "should not throw an error when inserting/updating/deleting a child with an incomplete foreign key"
  end

  describe "with a child_condition argument" do
    # Practical use of this?
  end
end
