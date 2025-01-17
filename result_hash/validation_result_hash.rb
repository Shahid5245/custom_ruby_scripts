# require_relative 'result_hash_v6_script'

def run_result_hash

  DisplayOptions.call('Choose the Entity', %w[Facility OrganizationAffiliation Provider PractitionerRole])
  entity_choice = gets.to_i
  return puts 'Wrong argument' unless entity_choice <= 4

  DisplayOptions.call('Choose the Tenant process', ['All tenants', 'One tenant', 'Particular tenant source'])
  tenant_choice = gets.to_i
  return puts 'Wrong argument' unless tenant_choice <= 3

  # create an instance varible of result_hash method into an array
  instance_of_all_result_hashes = [
    ->(tenant, source = nil) { ResultHash.result_hash_for_cluster_doc_org(tenant, source) },
    ->(tenant, source = nil) { ResultHash.result_hash_for_single_doc_org(tenant, source) },
    ->(tenant, source = nil) { ResultHash.result_hash_for_cluster_doc_pr(tenant, source) },
    ->(tenant, source = nil) { ResultHash.result_hash_for_single_doc_pr(tenant, source) }
  ]

  # execute the result_hash based on the condition
  validate_tenant_and_execute_resulthash = lambda do |tenant_choice, instance_of_result_hash|
    case tenant_choice
    when 1
      AllTenants.map { |tenant| puts "\n #{tenant}:"; instance_of_result_hash.call(tenant) }
    when 2
      DisplayOptions.call('Choose tenant', AllTenants)
      selected_tenant = AllTenants[gets.to_i - 1]
      instance_of_result_hash.call(selected_tenant)
    when 3
      DisplayOptions.call("Enter source name with tenant (e.g., 'wahbe, wa-amerigroup'):", nil)
      tenant, source = gets.chomp.split(',').map(&:strip)
      instance_of_result_hash.call(tenant, source)
    else
      puts 'Wrong argument'
    end
  end

  # passing the required resulthash to validate
  final_result_hash = validate_tenant_and_execute_resulthash.call(tenant_choice, instance_of_all_result_hashes[entity_choice - 1])
  puts JSON.generate(final_result_hash) if final_result_hash.present?
end
