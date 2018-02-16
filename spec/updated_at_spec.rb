require 'spec_helper'

describe PgTriggers, 'updated_at' do
  before do
    DB.drop_table? :updated_at_table

    DB.create_table :updated_at_table do
      primary_key :id

      integer :integer_column

      timestamptz :updated_at
    end

    DB.run PgTriggers.updated_at :updated_at_table, :updated_at
  end

  it "should set the updated_at time to now() when the row is inserted" do
    # The result of now() is the time the transaction began.
    t = nil
    DB.transaction do
      t = DB.get{now.function}
      DB[:updated_at_table].insert(integer_column: 1)
      DB[:updated_at_table].get(:updated_at).should == t
    end
    DB[:updated_at_table].get(:updated_at).should == t
  end

  it "should set the updated_at time to now() when the row is updated" do
    # The result of now() is the time the transaction began.
    t = nil
    id = DB[:updated_at_table].insert(integer_column: 1)
    DB.transaction do
      t = DB.get{now.function}
      DB[:updated_at_table].update integer_column: 2
      DB[:updated_at_table].get(:updated_at).should == t
    end
    DB[:updated_at_table].get(:updated_at).should == t
  end

  it "should set the updated_at time to now() when the row is updated, even if no values in the row change" do
    # The result of now() is the time the transaction began.
    DB[:updated_at_table].insert(integer_column: 1)
    t = nil
    DB.transaction do
      t = DB.get{now.function}
      DB[:updated_at_table].update integer_column: 1
      DB[:updated_at_table].get(:updated_at).should == t
    end
    DB[:updated_at_table].get(:updated_at).should == t
  end

  it "on insert should not overwrite a time the column is specifically set to" do
    # Handle loss of timestamp precision in DB roundtrip.
    t = DB.get Sequel.cast(Time.now - 30, :timestamptz)

    DB.transaction do
      DB[:updated_at_table].insert integer_column: 1, updated_at: t
      DB[:updated_at_table].get(:updated_at).should == t
    end

    DB[:updated_at_table].get(:updated_at).should == t
  end

  it "on update should not overwrite a time the column is specifically set to" do
    id = DB[:updated_at_table].insert(integer_column: 1)
    DB[:updated_at_table].get(:updated_at)

    # Handle loss of timestamp precision in DB roundtrip.
    t = DB.get Sequel.cast(Time.now - 30, :timestamptz)

    DB.transaction do
      DB[:updated_at_table].update integer_column: 1, updated_at: t
      DB[:updated_at_table].get(:updated_at).should == t
    end

    DB[:updated_at_table].get(:updated_at).should == t
  end
end
