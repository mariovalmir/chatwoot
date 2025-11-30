class AddWuzapiToWhatsappProviderConnectionIndex < ActiveRecord::Migration[7.1]
  disable_ddl_transaction!

  def up
    # Remove the old index
    remove_index :channel_whatsapp, name: 'index_channel_whatsapp_provider_connection', if_exists: true

    # Add new index including wuzapi alongside baileys and zapi
    add_index :channel_whatsapp, :provider_connection,
              using: :gin,
              where: "provider IN ('baileys', 'zapi', 'wuzapi')",
              name: 'index_channel_whatsapp_provider_connection',
              algorithm: :concurrently
  end

  def down
    # Revert to the previous index (without wuzapi)
    remove_index :channel_whatsapp, name: 'index_channel_whatsapp_provider_connection', if_exists: true

    add_index :channel_whatsapp, :provider_connection,
              using: :gin,
              where: "provider IN ('baileys', 'zapi')",
              name: 'index_channel_whatsapp_provider_connection',
              algorithm: :concurrently
  end
end
