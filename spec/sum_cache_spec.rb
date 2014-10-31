require 'spec_helper'

describe PgTriggers, 'sum_cache' do
  before do
    DB.drop_table?(:summer_table)
    DB.drop_table?(:summed_table)
  end

  it "should increment and decrement the count of related items in the associated table as rows are inserted and updated and deleted" do
    DB.create_table :summer_table do
      integer :id, null: false
      integer :summer_sum, null: false, default: 0
    end

    DB.create_table :summed_table do
      integer :id, null: false
      integer :summer_id, null: false
      integer :summed_count, null: false, default: 0
    end

    DB.run PgTriggers.sum_cache :summer_table, :summer_sum, :summed_table, :summed_count, {id: :summer_id}

    DB[:summer_table].insert(id: 1)
    DB[:summer_table].insert(id: 2)
    DB[:summer_table].where(id: 1).get(:summer_sum).should == 0

    DB[:summed_table].insert(id: 1, summer_id: 1)
    DB[:summer_table].where(id: 1).get(:summer_sum).should == 0

    DB[:summed_table].where(id: 1).update(:summed_count => 3).should == 1
    DB[:summer_table].where(id: 1).get(:summer_sum).should == 3

    DB[:summed_table].insert(id: 2, summer_id: 1)
    DB[:summer_table].where(id: 1).get(:summer_sum).should == 3

    DB[:summed_table].where(id: 2).update(:summed_count => 4).should == 1
    DB[:summer_table].where(id: 1).get(:summer_sum).should == 7

    DB[:summed_table].where(id: 1).delete.should == 1
    DB[:summer_table].where(id: 1).get(:summer_sum).should == 4

    DB[:summed_table].insert(id: 3, summer_id: 1, summed_count: 5)
    DB[:summer_table].where(id: 1).get(:summer_sum).should == 9
  end

  # it "should work when the tables are related by multiple columns" do
  #   DB.create_table :counter_table do
  #     integer :id1, null: false
  #     integer :id2, null: false
  #     integer :counted_count, null: false, default: 0
  #   end

  #   DB.create_table :counted_table do
  #     integer :id, null: false
  #     integer :counter_id1, null: false
  #     integer :counter_id2, null: false
  #   end

  #   DB.run PgTriggers.counter_cache :counter_table, :counted_count, :counted_table, {id1: :counter_id1, id2: :counter_id2}

  #   DB[:counter_table].insert(id1: 1, id2: 1)
  #   DB[:counter_table].insert(id1: 2, id2: 1)
  #   DB[:counter_table].where(id1: 1).get(:counted_count).should == 0

  #   DB[:counted_table].insert(id: 1, counter_id1: 1, counter_id2: 1)
  #   DB[:counter_table].where(id1: 1).get(:counted_count).should == 1

  #   DB[:counted_table].insert(id: 2, counter_id1: 1, counter_id2: 1)
  #   DB[:counter_table].where(id1: 1).get(:counted_count).should == 2

  #   DB[:counted_table].insert(id: 3, counter_id1: 2, counter_id2: 1)
  #   DB[:counter_table].where(id1: 1).get(:counted_count).should == 2
  #   DB[:counter_table].where(id1: 2).get(:counted_count).should == 1

  #   DB[:counted_table].insert(id: 4, counter_id1: 1, counter_id2: 1)
  #   DB[:counter_table].where(id1: 1).get(:counted_count).should == 3

  #   DB[:counted_table].where(id: 4).update(counter_id1: 2).should == 1
  #   DB[:counter_table].where(id1: 1).get(:counted_count).should == 2
  #   DB[:counter_table].where(id1: 2).get(:counted_count).should == 2

  #   DB[:counted_table].where(counter_id1: 1).delete.should == 2
  #   DB[:counter_table].where(id1: 1).get(:counted_count).should == 0
  # end

  # it "should work as expected when any of the columns are set to null or an unknown value" do
  #   DB.create_table :counter_table do
  #     integer :id1, null: false
  #     integer :id2, null: false
  #     integer :counted1_count, null: false, default: 0
  #     integer :counted2_count, null: false, default: 0
  #   end

  #   DB.create_table :counted_table do
  #     integer :id, null: false
  #     integer :counter_id1
  #     integer :counter_id2
  #   end

  #   DB.run PgTriggers.counter_cache :counter_table, :counted1_count, :counted_table, {id1: :counter_id1}
  #   DB.run PgTriggers.counter_cache :counter_table, :counted2_count, :counted_table, {id1: :counter_id1, id2: :counter_id2}

  #   DB[:counter_table].insert(id1: 1, id2: 1)
  #   DB[:counted_table].insert(id: 1, counter_id1: 1, counter_id2: 1)
  #   DB[:counted_table].insert(id: 2, counter_id1: 1, counter_id2: 1)
  #   DB[:counted_table].insert(id: 3, counter_id1: 1, counter_id2: 1)
  #   DB[:counted_table].insert(id: 4, counter_id1: 1, counter_id2: 1)
  #   DB[:counter_table].where(id1: 1, id2: 1).get([:counted1_count, :counted2_count]).should == [4, 4]

  #   DB[:counted_table].where(id: 1).update(counter_id1: nil).should == 1
  #   DB[:counter_table].where(id1: 1, id2: 1).get([:counted1_count, :counted2_count]).should == [3, 3]

  #   DB[:counted_table].where(id: 2).update(counter_id2: nil).should == 1
  #   DB[:counter_table].where(id1: 1, id2: 1).get([:counted1_count, :counted2_count]).should == [3, 2]

  #   DB[:counted_table].where(id: 3).update(counter_id1: 90000000).should == 1
  #   DB[:counter_table].where(id1: 1, id2: 1).get([:counted1_count, :counted2_count]).should == [2, 1]

  #   DB[:counted_table].where(id: 4).update(counter_id2: 90000000).should == 1
  #   DB[:counter_table].where(id1: 1, id2: 1).get([:counted1_count, :counted2_count]).should == [2, 0]

  #   DB[:counted_table].insert(id: 5, counter_id1: 1, counter_id2: nil)
  #   DB[:counter_table].where(id1: 1, id2: 1).get([:counted1_count, :counted2_count]).should == [3, 0]

  #   DB[:counted_table].insert(id: 6, counter_id1: nil, counter_id2: nil)
  #   DB[:counter_table].where(id1: 1, id2: 1).get([:counted1_count, :counted2_count]).should == [3, 0]
  # end

  # it "should accept a :where clause to filter the rows that are counted" do
  #   DB.create_table :counter_table do
  #     integer :id, null: false

  #     integer :condition_count,       null: false, default: 0
  #     integer :value_count,           null: false, default: 0
  #     integer :condition_value_count, null: false, default: 0
  #   end

  #   DB.create_table :counted_table do
  #     integer :id, null: false
  #     integer :counter_id, null: false

  #     boolean :condition
  #     integer :value
  #   end

  #   DB.run PgTriggers.counter_cache :counter_table, :condition_count,       :counted_table, {id: :counter_id}, where: "ROW.condition"
  #   DB.run PgTriggers.counter_cache :counter_table, :value_count,           :counted_table, {id: :counter_id}, where: "ROW.value > 5"
  #   DB.run PgTriggers.counter_cache :counter_table, :condition_value_count, :counted_table, {id: :counter_id}, where: "ROW.condition AND ROW.value > 5"

  #   def values
  #     DB[:counter_table].where(id: 1).get([:condition_count, :value_count, :condition_value_count])
  #   end

  #   DB[:counter_table].insert(id: 1)

  #   values.should == [0, 0, 0]
  #   DB[:counted_table].insert(id: 1, counter_id: 1, condition: true,  value: 4)
  #   values.should == [1, 0, 0]
  #   DB[:counted_table].insert(id: 2, counter_id: 1, condition: false, value: 4)
  #   values.should == [1, 0, 0]
  #   DB[:counted_table].insert(id: 3, counter_id: 1, condition: true,  value: 6)
  #   values.should == [2, 1, 1]
  #   DB[:counted_table].insert(id: 4, counter_id: 1, condition: false, value: 6)
  #   values.should == [2, 2, 1]
  #   DB[:counted_table].insert(id: 5, counter_id: 1, condition: nil,   value: 4)
  #   values.should == [2, 2, 1]
  #   DB[:counted_table].insert(id: 6, counter_id: 1, condition: false, value: nil)
  #   values.should == [2, 2, 1]
  #   DB[:counted_table].insert(id: 7, counter_id: 1, condition: true,  value: nil)
  #   values.should == [3, 2, 1]
  #   DB[:counted_table].insert(id: 8, counter_id: 1, condition: nil,   value: 6)
  #   values.should == [3, 3, 1]

  #   DB[:counted_table].where(id: 3).update(counter_id: 2).should == 1
  #   values.should == [2, 2, 0]
  #   DB[:counted_table].where(id: [4, 7]).delete.should == 2
  #   values.should == [1, 1, 0]
  #   DB[:counted_table].where(id: 3).update(counter_id: 1).should == 1
  #   values.should == [2, 2, 1]
  #   DB[:counted_table].where(id: 1).update(value: 6).should == 1
  #   values.should == [2, 3, 2]
  # end

  # it "should silently replace another counter cache trigger on the same set of columns" do
  #   DB.create_table :counter_table do
  #     integer :id, null: false

  #     integer :condition_count,       null: false, default: 0
  #   end

  #   DB.create_table :counted_table do
  #     integer :id, null: false
  #     integer :counter_id, null: false

  #     boolean :condition
  #   end

  #   def value
  #     DB[:counter_table].where(id: 1).get(:condition_count)
  #   end

  #   DB.run PgTriggers.counter_cache :counter_table, :condition_count, :counted_table, {id: :counter_id}

  #   DB[:counter_table].insert(id: 1)

  #   DB[:counted_table].insert(id: 1, counter_id: 1, condition: true)
  #   value.should == 1
  #   DB[:counted_table].insert(id: 2, counter_id: 1, condition: false)
  #   value.should == 2
  #   DB[:counted_table].insert(id: 3, counter_id: 1, condition: true)
  #   value.should == 3
  #   DB[:counted_table].insert(id: 4, counter_id: 1, condition: false)
  #   value.should == 4

  #   DB.run PgTriggers.counter_cache :counter_table, :condition_count, :counted_table, {id: :counter_id}, where: "ROW.condition"

  #   DB[:counted_table].insert(id: 5, counter_id: 1, condition: nil)
  #   value.should == 4
  #   DB[:counted_table].insert(id: 6, counter_id: 1, condition: false)
  #   value.should == 4
  #   DB[:counted_table].insert(id: 7, counter_id: 1, condition: true)
  #   value.should == 5
  #   DB[:counted_table].insert(id: 8, counter_id: 1, condition: nil)
  #   value.should == 5
  # end

  it "should support a custom multiplier to apply to the sum" do
    DB.create_table :summer_table do
      integer :id, null: false
      integer :summer_sum, null: false, default: 0
    end

    DB.create_table :summed_table do
      integer :id, null: false
      integer :summer_id, null: false
      integer :summed_count, null: false, default: 0
    end

    DB.run PgTriggers.sum_cache :summer_table, :summer_sum, :summed_table, :summed_count, {id: :summer_id}, multiplier: 4

    DB[:summer_table].insert(id: 1)
    DB[:summer_table].insert(id: 2)
    DB[:summer_table].where(id: 1).get(:summer_sum).should == 0

    DB[:summed_table].insert(id: 1, summer_id: 1)
    DB[:summer_table].where(id: 1).get(:summer_sum).should == 0

    DB[:summed_table].where(id: 1).update(:summed_count => 3).should == 1
    DB[:summer_table].where(id: 1).get(:summer_sum).should == 12

    DB[:summed_table].insert(id: 2, summer_id: 1)
    DB[:summer_table].where(id: 1).get(:summer_sum).should == 12

    DB[:summed_table].where(id: 2).update(:summed_count => 4).should == 1
    DB[:summer_table].where(id: 1).get(:summer_sum).should == 28

    DB[:summed_table].where(id: 1).delete.should == 1
    DB[:summer_table].where(id: 1).get(:summer_sum).should == 16

    DB[:summed_table].insert(id: 3, summer_id: 1, summed_count: 5)
    DB[:summer_table].where(id: 1).get(:summer_sum).should == 36
  end
end
