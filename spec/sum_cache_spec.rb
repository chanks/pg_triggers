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

    DB.run \
      PgTriggers.sum_cache(
        summing_table: :summer_table,
        summing_column: :summer_sum,
        summed_table: :summed_table,
        summed_column: :summed_count,
        relationship: {id: :summer_id},
      )

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

    DB[:summed_table].where(id: 2).update(:summed_count => 6).should == 1
    DB[:summer_table].where(id: 1).get(:summer_sum).should == 9

    DB[:summed_table].where(id: 1).delete.should == 1
    DB[:summer_table].where(id: 1).get(:summer_sum).should == 6

    DB[:summed_table].insert(id: 3, summer_id: 1, summed_count: 5)
    DB[:summer_table].where(id: 1).get(:summer_sum).should == 11
  end

  it "should work when the tables are related by multiple columns" do
    DB.create_table :summer_table do
      integer :id1, null: false
      integer :id2, null: false
      integer :summer_sum, null: false, default: 0
    end

    DB.create_table :summed_table do
      integer :id, null: false
      integer :summer_id1, null: false
      integer :summer_id2, null: false
      integer :summed_count, null: false, default: 0
    end

    DB.run \
      PgTriggers.sum_cache(
        summing_table: :summer_table,
        summing_column: :summer_sum,
        summed_table: :summed_table,
        summed_column: :summed_count,
        relationship: {id1: :summer_id1, id2: :summer_id2},
      )

    DB[:summer_table].insert(id1: 1, id2: 1)
    DB[:summer_table].insert(id1: 2, id2: 1)
    DB[:summer_table].where(id1: 1).get(:summer_sum).should == 0

    DB[:summed_table].insert(id: 1, summer_id1: 1, summer_id2: 1, summed_count: 2)
    DB[:summer_table].where(id1: 1).get(:summer_sum).should == 2

    DB[:summed_table].insert(id: 2, summer_id1: 1, summer_id2: 1, summed_count: 3)
    DB[:summer_table].where(id1: 1).get(:summer_sum).should == 5

    DB[:summed_table].insert(id: 3, summer_id1: 2, summer_id2: 1, summed_count: 4)
    DB[:summer_table].where(id1: 1).get(:summer_sum).should == 5
    DB[:summer_table].where(id1: 2).get(:summer_sum).should == 4

    DB[:summed_table].insert(id: 4, summer_id1: 1, summer_id2: 1, summed_count: 5)
    DB[:summer_table].where(id1: 1).get(:summer_sum).should == 10

    DB[:summed_table].where(id: 4).update(summer_id1: 2).should == 1
    DB[:summer_table].where(id1: 1).get(:summer_sum).should == 5
    DB[:summer_table].where(id1: 2).get(:summer_sum).should == 9

    DB[:summed_table].where(summer_id1: 1).delete.should == 2
    DB[:summer_table].where(id1: 1).get(:summer_sum).should == 0
  end

  it "should work as expected when any of the columns are set to null or an unknown value" do
    DB.create_table :summer_table do
      integer :id1, null: false
      integer :id2, null: false
      integer :summed1_sum, null: false, default: 0
      integer :summed2_sum, null: false, default: 0
    end

    DB.create_table :summed_table do
      integer :id, null: false
      integer :summer_id1
      integer :summer_id2
      integer :summed_count, null: false, default: 0
    end

    DB.run \
      PgTriggers.sum_cache(
        summing_table: :summer_table,
        summing_column: :summed1_sum,
        summed_table: :summed_table,
        summed_column: :summed_count,
        relationship: {id1: :summer_id1},
      )

    DB.run \
      PgTriggers.sum_cache(
        summing_table: :summer_table,
        summing_column: :summed2_sum,
        summed_table: :summed_table,
        summed_column: :summed_count,
        relationship: {id1: :summer_id1, id2: :summer_id2},
      )

    DB[:summer_table].insert(id1: 1, id2: 1)
    DB[:summed_table].insert(id: 1, summer_id1: 1, summer_id2: 1, summed_count: 1)
    DB[:summed_table].insert(id: 2, summer_id1: 1, summer_id2: 1, summed_count: 2)
    DB[:summed_table].insert(id: 3, summer_id1: 1, summer_id2: 1, summed_count: 4)
    DB[:summed_table].insert(id: 4, summer_id1: 1, summer_id2: 1, summed_count: 8)
    DB[:summer_table].where(id1: 1, id2: 1).get([:summed1_sum, :summed2_sum]).should == [15, 15]

    DB[:summed_table].where(id: 1).update(summer_id1: nil).should == 1
    DB[:summer_table].where(id1: 1, id2: 1).get([:summed1_sum, :summed2_sum]).should == [14, 14]

    DB[:summed_table].where(id: 2).update(summer_id2: nil).should == 1
    DB[:summer_table].where(id1: 1, id2: 1).get([:summed1_sum, :summed2_sum]).should == [14, 12]

    DB[:summed_table].where(id: 3).update(summer_id1: 90000000).should == 1
    DB[:summer_table].where(id1: 1, id2: 1).get([:summed1_sum, :summed2_sum]).should == [10, 8]

    DB[:summed_table].where(id: 4).update(summer_id2: 90000000).should == 1
    DB[:summer_table].where(id1: 1, id2: 1).get([:summed1_sum, :summed2_sum]).should == [10, 0]

    DB[:summed_table].insert(id: 5, summer_id1: 1, summer_id2: nil, summed_count: 16)
    DB[:summer_table].where(id1: 1, id2: 1).get([:summed1_sum, :summed2_sum]).should == [26, 0]

    DB[:summed_table].insert(id: 6, summer_id1: nil, summer_id2: nil, summed_count: 32)
    DB[:summer_table].where(id1: 1, id2: 1).get([:summed1_sum, :summed2_sum]).should == [26, 0]
  end

  it "should accept a :where clause to filter the rows that are counted" do
    DB.create_table :summer_table do
      integer :id, null: false

      integer :condition_sum,       null: false, default: 0
      integer :value_sum,           null: false, default: 0
      integer :condition_value_sum, null: false, default: 0
    end

    DB.create_table :summed_table do
      integer :id, null: false
      integer :summer_id, null: false
      integer :summed_count, null: false, default: 0

      boolean :condition
      integer :value
    end

    DB.run \
      PgTriggers.sum_cache(
        summing_table: :summer_table,
        summing_column: :condition_sum,
        summed_table: :summed_table,
        summed_column: :summed_count,
        relationship: {id: :summer_id},
        where: "ROW.condition",
      )

    DB.run \
      PgTriggers.sum_cache(
        summing_table: :summer_table,
        summing_column: :value_sum,
        summed_table: :summed_table,
        summed_column: :summed_count,
        relationship: {id: :summer_id},
        where: "ROW.value > 5",
      )

    DB.run \
      PgTriggers.sum_cache(
        summing_table: :summer_table,
        summing_column: :condition_value_sum,
        summed_table: :summed_table,
        summed_column: :summed_count,
        relationship: {id: :summer_id},
        where: "ROW.condition AND ROW.value > 5",
      )

    def values
      DB[:summer_table].where(id: 1).get([:condition_sum, :value_sum, :condition_value_sum])
    end

    DB[:summer_table].insert(id: 1)

    values.should == [0, 0, 0]
    DB[:summed_table].insert(id: 1, summer_id: 1, condition: true,  value: 4, summed_count: 1)
    values.should == [1, 0, 0]
    DB[:summed_table].insert(id: 2, summer_id: 1, condition: false, value: 4, summed_count: 2)
    values.should == [1, 0, 0]
    DB[:summed_table].insert(id: 3, summer_id: 1, condition: true,  value: 6, summed_count: 4)
    values.should == [5, 4, 4]
    DB[:summed_table].insert(id: 4, summer_id: 1, condition: false, value: 6, summed_count: 8)
    values.should == [5, 12, 4]
    DB[:summed_table].insert(id: 5, summer_id: 1, condition: nil,   value: 4, summed_count: 16)
    values.should == [5, 12, 4]
    DB[:summed_table].insert(id: 6, summer_id: 1, condition: false, value: nil, summed_count: 32)
    values.should == [5, 12, 4]
    DB[:summed_table].insert(id: 7, summer_id: 1, condition: true,  value: nil, summed_count: 64)
    values.should == [69, 12, 4]
    DB[:summed_table].insert(id: 8, summer_id: 1, condition: nil,   value: 6, summed_count: 128)
    values.should == [69, 140, 4]

    DB[:summed_table].where(id: 3).update(summer_id: 2).should == 1
    values.should == [65, 136, 0]
    DB[:summed_table].where(id: [4, 7]).delete.should == 2
    values.should == [1, 128, 0]
    DB[:summed_table].where(id: 3).update(summer_id: 1).should == 1
    values.should == [5, 132, 4]
    DB[:summed_table].where(id: 1).update(value: 6).should == 1
    values.should == [5, 133, 5]
  end

  it "should silently replace another counter cache trigger on the same set of columns" do
    DB.create_table :summer_table do
      integer :id, null: false
      integer :condition_sum, null: false, default: 0
    end

    DB.create_table :summed_table do
      integer :id, null: false
      integer :summer_id, null: false
      integer :summed_count, null: false, default: 0
      boolean :condition
    end

    def value
      DB[:summer_table].where(id: 1).get(:condition_sum)
    end

    DB.run \
      PgTriggers.sum_cache(
        summing_table: :summer_table,
        summing_column: :condition_sum,
        summed_table: :summed_table,
        summed_column: :summed_count,
        relationship: {id: :summer_id},
      )

    DB[:summer_table].insert(id: 1)

    DB[:summed_table].insert(id: 1, summer_id: 1, summed_count: 1, condition: true)
    value.should == 1
    DB[:summed_table].insert(id: 2, summer_id: 1, summed_count: 2, condition: false)
    value.should == 3
    DB[:summed_table].insert(id: 3, summer_id: 1, summed_count: 4, condition: true)
    value.should == 7
    DB[:summed_table].insert(id: 4, summer_id: 1, summed_count: 8, condition: false)
    value.should == 15

    DB.run \
      PgTriggers.sum_cache(
        summing_table: :summer_table,
        summing_column: :condition_sum,
        summed_table: :summed_table,
        summed_column: :summed_count,
        relationship: {id: :summer_id},
        where: "ROW.condition",
      )

    DB[:summed_table].insert(id: 5, summer_id: 1, summed_count: 16, condition: nil)
    value.should == 15
    DB[:summed_table].insert(id: 6, summer_id: 1, summed_count: 32, condition: false)
    value.should == 15
    DB[:summed_table].insert(id: 7, summer_id: 1, summed_count: 64, condition: true)
    value.should == 79
    DB[:summed_table].insert(id: 8, summer_id: 1, summed_count: 128, condition: nil)
    value.should == 79
  end

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

    DB.run \
      PgTriggers.sum_cache(
        summing_table: :summer_table,
        summing_column: :summer_sum,
        summed_table: :summed_table,
        summed_column: :summed_count,
        relationship: {id: :summer_id},
        multiplier: 4,
      )

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

    DB[:summed_table].where(id: 2).update(:summed_count => 6).should == 1
    DB[:summer_table].where(id: 1).get(:summer_sum).should == 36

    DB[:summed_table].where(id: 1).delete.should == 1
    DB[:summer_table].where(id: 1).get(:summer_sum).should == 24

    DB[:summed_table].insert(id: 3, summer_id: 1, summed_count: 5)
    DB[:summer_table].where(id: 1).get(:summer_sum).should == 44
  end
end
