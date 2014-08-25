require 'sequel'
require 'pg_triggers'
require 'pry'

url = ENV['PG_TRIGGERS_URL'] || 'postgres:///pg_triggers'

DB = Sequel.connect(url)

RSpec.configure do |config|
  config.expect_with(:rspec) { |c| c.syntax = [:expect, :should] }

  config.around do |example|
    DB.transaction(rollback: :always, auto_savepoint: true) { example.run }
  end
end
