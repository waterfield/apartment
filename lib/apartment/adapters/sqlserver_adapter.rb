require 'apartment/adapters/abstract_adapter'

module Apartment

  module Tenant

    def self.sqlserver_adapter(config)
      adapter = Adapters::SQLServerAdapter
      # adapter = Adapters::SQLServerSchemaAdapter if Apartment.use_schemas
      # adapter = Adapters::SQLServerSchemaFromSqlAdapter if Apartment.use_sql && Apartment.use_schemas
      adapter.new(config)
    end
  end

  module Adapters
    class SQLServerAdapter < AbstractAdapter

      # Initialize the adapter with tenant configuration
      def initialize(tenant_config)
        super
        # puts tenant_config.inspect
        @tenant_config = {}
        @default_tenant = tenant_config
        @tenant_config[:default_tenant] = tenant_config # The full tenant configuration passed in Apartment initializer
        @current_tenant = nil
      end

      # Switch to a given tenant by establishing a new connection to the tenant's database
      def switch!(tenant)
        return if tenant == @current_tenant
        @tenant_config[tenant] = db_connection_config(tenant) unless @tenant_config[tenant].present?


        # Ensure we reset the connection if switching tenants
        # reset if @current_tenant

        tenant_config = @tenant_config[tenant]
        raise TenantNotFound, "Configuration for tenant '#{tenant}' not found" unless tenant_config

        # Re-establish the database connection with tenant-specific config
        establish_connection(tenant_config)

        @current_tenant = tenant
      rescue ActiveRecord::StatementInvalid => e
        raise TenantNotFound, "Error switching tenant: #{tenant} - #{e.message}"
      end

      # Reset the connection back to the default or disconnect
      def reset
        return unless @current_tenant

        # Close the connection to the tenant's database and restore the default connection
        ActiveRecord::Base.connection_pool.disconnect!
        establish_connection(default_database_config)

        @current_ttenant = nil
      end

      # Create method won't be used in this case as we don't manage schemas, we switch whole databases
      def create(_tenant)
        raise "Tenant creation is not supported in this setup as databases are managed externally."
      end

      # Drop method won't be used in this case since databases are external
      def drop(_tenant)
        raise "Tenant deletion is not supported in this setup as databases are managed externally."
      end

      # Define how to retrieve tenant names - in this case, from the config hash
      def tenant_names
        @tenant_config.keys
      end

      # Running migrations across all tenants - this connects to each tenant's database to run migrations
      def process_migrations
        tenant_names.each do |tenant|
          switch!(tenant) do
            ActiveRecord::Migrator.migrate('db/migrate')
          end
        end
      rescue StandardError => e
        raise "Error running migrations for tenant: #{tenant} - #{e.message}"
      ensure
        reset
      end

      # Custom exceptions for tenant handling
      class TenantNotFound < StandardError; end

      private

      # Establish connection to a specific database using tenant configuration
      def establish_connection(config)
        ActiveRecord::Base.establish_connection(config)
      end

      # Define the default database connection settings (optional, for the fallback case)
      def default_database_config
        @default_tenant
      end
    end
  end
end
