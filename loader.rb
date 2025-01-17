# require 'open-uri'

AllTenants = Utils::Property.new.get_all_active_tenant_key - ['qa-tenant']
OrgAffProfile = PROFILE_URL['organization_affiliation']
PrRoleProfile = PROFILE_URL['practitioner_role']
NppesProfile = PROFILE_URL['nppes_master']
ChewyClientIndex = Chewy.client.indices
ElasticClientIndex = ELASTICSEARCH_CLIENT.indices

# Display options
DisplayOptions = ->(title, options) {
  puts "\n\033[33m#{title}:\033[0m" if title.present?
  options.each_with_index { |o,i| puts "#{i+1}. #{o.capitalize}" } if options.present?
}

def load_file
  git_base_url = 'https://raw.githubusercontent.com/Shahid5245/custom_ruby_scripts/main/'
  files_to_load = %w[result_hash/result_hash_v6_script.rb result_hash/validation_result_hash.rb my_custom_methods.rb]
  DisplayOptions.call(nil, ['Load result hash', 'Load Custom script'])
  load_choice = gets.to_i
  files_to_load = load_choice == 2 ? [files_to_load.last] : files_to_load
  files_to_load.each { |filename| eval(URI.open(git_base_url + filename).read) }
end;1

load_file
nil
