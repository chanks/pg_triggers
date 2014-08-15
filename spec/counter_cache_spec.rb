require 'spec_helper'

describe PgTriggers, 'counter_cache' do
  before do
    DB.drop_table?(:counter_table)
    DB.drop_table?(:counted_table)
  end

  it "should increment and decrement the count of related items in the associated table as rows are inserted and updated and deleted" do
    DB.create_table :counter_table do
      integer :id, null: false
      integer :counted_count, null: false, default: 0
    end

    DB.create_table :counted_table do
      integer :id, null: false
      integer :counter_id, null: false
    end

    DB.run PgTriggers.counter_cache :counter_table, :counted_count, :counted_table, {id: :counter_id}

    DB[:counter_table].insert(id: 1)
    DB[:counter_table].insert(id: 2)
    DB[:counter_table].where(id: 1).get(:counted_count).should == 0

    DB[:counted_table].insert(id: 1, counter_id: 1)
    DB[:counter_table].where(id: 1).get(:counted_count).should == 1

    DB[:counted_table].insert(id: 2, counter_id: 1)
    DB[:counter_table].where(id: 1).get(:counted_count).should == 2

    DB[:counted_table].insert(id: 3, counter_id: 2)
    DB[:counter_table].where(id: 1).get(:counted_count).should == 2
    DB[:counter_table].where(id: 2).get(:counted_count).should == 1

    DB[:counted_table].insert(id: 4, counter_id: 1)
    DB[:counter_table].where(id: 1).get(:counted_count).should == 3

    DB[:counted_table].where(id: 4).update(counter_id: 2).should == 1
    DB[:counter_table].where(id: 1).get(:counted_count).should == 2
    DB[:counter_table].where(id: 2).get(:counted_count).should == 2

    DB[:counted_table].where(counter_id: 1).delete.should == 2
    DB[:counter_table].where(id: 1).get(:counted_count).should == 0
  end

  it "should work when the tables are related by multiple columns" do
    DB.create_table :counter_table do
      integer :id1, null: false
      integer :id2, null: false
      integer :counted_count, null: false, default: 0
    end

    DB.create_table :counted_table do
      integer :id, null: false
      integer :counter_id1, null: false
      integer :counter_id2, null: false
    end

    DB.run PgTriggers.counter_cache :counter_table, :counted_count, :counted_table, {id1: :counter_id1, id2: :counter_id2}

    DB[:counter_table].insert(id1: 1, id2: 1)
    DB[:counter_table].insert(id1: 2, id2: 1)
    DB[:counter_table].where(id1: 1).get(:counted_count).should == 0

    DB[:counted_table].insert(id: 1, counter_id1: 1, counter_id2: 1)
    DB[:counter_table].where(id1: 1).get(:counted_count).should == 1

    DB[:counted_table].insert(id: 2, counter_id1: 1, counter_id2: 1)
    DB[:counter_table].where(id1: 1).get(:counted_count).should == 2

    DB[:counted_table].insert(id: 3, counter_id1: 2, counter_id2: 1)
    DB[:counter_table].where(id1: 1).get(:counted_count).should == 2
    DB[:counter_table].where(id1: 2).get(:counted_count).should == 1

    DB[:counted_table].insert(id: 4, counter_id1: 1, counter_id2: 1)
    DB[:counter_table].where(id1: 1).get(:counted_count).should == 3

    DB[:counted_table].where(id: 4).update(counter_id1: 2).should == 1
    DB[:counter_table].where(id1: 1).get(:counted_count).should == 2
    DB[:counter_table].where(id1: 2).get(:counted_count).should == 2

    DB[:counted_table].where(counter_id1: 1).delete.should == 2
    DB[:counter_table].where(id1: 1).get(:counted_count).should == 0
  end
end
