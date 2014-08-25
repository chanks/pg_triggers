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

    it "should record old versions of rows when they are updated" do
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

    it "should always record the values of columns in the :always set" do
      DB.run PgTriggers.audit_table(:audited_table, include: [:id, :item_count])

      id = DB[:audited_table].insert
      DB[:audited_table].where(id: id).update item_count: 1
      DB[:audited_table].where(id: id).update description: 'blah'

      DB[:audit_table].count.should == 2
      r1, r2 = DB[:audit_table].order(:id).all

      r1[:id].should == 1
      r1[:table_name].should == 'audited_table'
      r1[:changed_at].should be_within(3).of Time.now
      JSON.parse(r1[:changes]).should == {'id' => 1, 'item_count' => 0}

      r2[:id].should == 2
      r2[:table_name].should == 'audited_table'
      r2[:changed_at].should be_within(3).of Time.now
      JSON.parse(r2[:changes]).should == {'id' => 1, 'item_count' => 1, 'description' => nil}
    end

    it "should not record UPDATEs when the only changed columns fall within the :ignore set" do
      DB.run PgTriggers.audit_table(:audited_table, ignore: [:item_count])

      id = DB[:audited_table].insert
      DB[:audited_table].where(id: id).update item_count: 1
      DB[:audited_table].where(id: id).update description: 'blah'
      DB[:audited_table].where(id: id).update description: nil
      DB[:audited_table].where(id: id).update item_count: 2

      DB[:audit_table].count.should == 2
      r1, r2 = DB[:audit_table].order(:id).all

      r1[:id].should == 1
      r1[:table_name].should == 'audited_table'
      r1[:changed_at].should be_within(3).of Time.now
      JSON.parse(r1[:changes]).should == {'description' => nil}

      r2[:id].should == 2
      r2[:table_name].should == 'audited_table'
      r2[:changed_at].should be_within(3).of Time.now
      JSON.parse(r2[:changes]).should == {'description' => 'blah'}
    end

    it "should handle columns being in both the :include and :ignore sets properly" do
      DB.run PgTriggers.audit_table(:audited_table, include: [:item_count], ignore: [:item_count])

      id = DB[:audited_table].insert
      DB[:audited_table].where(id: id).update item_count: 1
      DB[:audited_table].where(id: id).update description: 'blah'
      DB[:audited_table].where(id: id).update description: nil
      DB[:audited_table].where(id: id).update item_count: 2

      DB[:audit_table].count.should == 2
      r1, r2 = DB[:audit_table].order(:id).all

      r1[:id].should == 1
      r1[:table_name].should == 'audited_table'
      r1[:changed_at].should be_within(3).of Time.now
      JSON.parse(r1[:changes]).should == {'description' => nil, 'item_count' => 1}

      r2[:id].should == 2
      r2[:table_name].should == 'audited_table'
      r2[:changed_at].should be_within(3).of Time.now
      JSON.parse(r2[:changes]).should == {'description' => 'blah', 'item_count' => 1}
    end

    it "should ignore records that are not changed at all" do
      DB.run PgTriggers.audit_table(:audited_table)

      id = DB[:audited_table].insert
      DB[:audited_table].where(id: id).update item_count: 0
      DB[:audit_table].count.should == 0
    end

    it "should not include changed columns if they are ignored" do
      DB.run PgTriggers.audit_table(:audited_table, ignore: [:item_count])

      id = DB[:audited_table].insert
      DB[:audited_table].where(id: id).update description: 'blah', item_count: 1
      DB[:audited_table].where(id: id).update description: 'blah', item_count: 2
      DB[:audited_table].where(id: id).update description: nil, item_count: 3

      DB[:audit_table].count.should == 2
      r1, r2 = DB[:audit_table].order(:id).all

      r1[:id].should == 1
      r1[:table_name].should == 'audited_table'
      r1[:changed_at].should be_within(3).of Time.now
      JSON.parse(r1[:changes]).should == {'description' => nil}

      r2[:id].should == 2
      r2[:table_name].should == 'audited_table'
      r2[:changed_at].should be_within(3).of Time.now
      JSON.parse(r2[:changes]).should == {'description' => 'blah'}
    end

    it "should record the entirety of the row when it is deleted" do
      DB.run PgTriggers.audit_table(:audited_table)

      id = DB[:audited_table].insert description: 'Go home and get your shinebox!', item_count: 5
      DB[:audited_table].where(id: id).delete.should == 1

      DB[:audit_table].count.should == 1
      record = DB[:audit_table].first
      record[:id].should == 1
      record[:table_name].should == 'audited_table'
      record[:changed_at].should be_within(3).of Time.now
      JSON.parse(record[:changes]).should == {'id' => 1, 'description' => 'Go home and get your shinebox!', 'item_count' => 5}
    end

    it "should seamlessly replace an existing audit trigger on the same table" do
      id = DB[:audited_table].insert description: 'Go home and get your shinebox!', item_count: 5

      DB.run PgTriggers.audit_table(:audited_table)
      DB[:audited_table].where(id: id).update(item_count: 6)
      DB[:audit_table].count.should == 1

      DB.run PgTriggers.audit_table(:audited_table, ignore: [:item_count])
      DB[:audited_table].where(id: id).update(item_count: 7)
      DB[:audit_table].count.should == 1
      DB[:audited_table].where(id: id).update(description: 'blah')
      DB[:audit_table].count.should == 2
    end

    it "should properly handle rows of type JSON" do
      DB.alter_table :audited_table do
        add_column :data, :json, null: false, default: '{}'
      end

      id = DB[:audited_table].insert data: '{}'

      DB.run PgTriggers.audit_table(:audited_table)
      DB[:audited_table].where(id: id).update(data: '{"a":1}')
      DB[:audited_table].where(id: id).update(data: '{"a":1,"b":2}')
      DB[:audited_table].where(id: id).update(data: '{"a":1,"b":2}')
      DB[:audited_table].where(id: id).update(data: '{"a":2,"b":2}')

      DB.run PgTriggers.audit_table(:audited_table, ignore: [:data])
      DB[:audited_table].where(id: id).update(data: '{"a":8}')

      DB[:audit_table].count.should == 3
      r1, r2, r3 = DB[:audit_table].all

      r1[:id].should == 1
      r1[:table_name].should == 'audited_table'
      r1[:changed_at].should be_within(3).of Time.now
      JSON.parse(r1[:changes]).should == {'data' => {}}

      r2[:id].should == 2
      r2[:table_name].should == 'audited_table'
      r2[:changed_at].should be_within(3).of Time.now
      JSON.parse(r2[:changes]).should == {'data' => {'a' => 1}}

      r3[:id].should == 3
      r3[:table_name].should == 'audited_table'
      r3[:changed_at].should be_within(3).of Time.now
      JSON.parse(r3[:changes]).should == {'data' => {'a' => 1, 'b' => 2}}
    end
  end
end
