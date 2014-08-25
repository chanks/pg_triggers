require 'spec_helper'
require 'json'

describe PgTriggers, 'auditing' do
  before do
    DB.drop_table?(:audit_table)
  end

  describe "create_audit_table" do
    it "should create a table to hold auditing information" do
      DB.run PgTriggers.create_audit_table
      DB.table_exists?(:audit_table).should be true
    end
  end

  describe "audit_table" do
    before do
      DB.run PgTriggers.create_audit_table
      DB.drop_table?(:audited_table)
      DB.create_table :audited_table do
        primary_key :id
        text :description
        integer :item_count, null: false, default: 0
      end
    end

    it "should record old versions of rows when they are updated and deleted" do
      DB.run PgTriggers.audit_table(:audited_table)

      id = DB[:audited_table].insert
      DB[:audited_table].where(id: id).update item_count: 1
      DB[:audited_table].where(id: id).update description: 'blah'
      DB[:audited_table].where(id: id).update description: nil
      DB[:audited_table].where(id: id).update item_count: 2

      DB[:audit_table].count.should == 4
      r1, r2, r3, r4 = DB[:audit_table].order(:id).all

      r1[:id].should == 1
      r1[:table_name].should == 'audited_table'
      r1[:changed_at].should be_within(3).of Time.now
      JSON.parse(r1[:changes]).should == {'item_count' => 0}

      r2[:id].should == 2
      r2[:table_name].should == 'audited_table'
      r2[:changed_at].should be_within(3).of Time.now
      JSON.parse(r2[:changes]).should == {'description' => nil}

      r3[:id].should == 3
      r3[:table_name].should == 'audited_table'
      r3[:changed_at].should be_within(3).of Time.now
      JSON.parse(r3[:changes]).should == {'description' => 'blah'}

      r4[:id].should == 4
      r4[:table_name].should == 'audited_table'
      r4[:changed_at].should be_within(3).of Time.now
      JSON.parse(r4[:changes]).should == {'item_count' => 1}
    end

    it "should not record UPDATEs when the only changed columns fall within the :ignore set"

    it "should seamlessly replace an existing audit trigger on the same table"
  end
end
