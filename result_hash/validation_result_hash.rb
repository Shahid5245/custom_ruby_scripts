# require_relative 'result_hash_v6_script'

def run_result_hash
  @yellow = "\033[33m"
  @reset = "\033[0m"
  tenants = Utils::Property.new.get_all_active_tenant_key.reject { |i| i == 'qa-tenant' }
  final_result_hash = ""
  
  # Display options
  display_options = ->(title, options) do
    puts "\n#{@yellow}#{title}:#{@reset}"
    options.each_with_index {|o,i| puts "#{i+1}. #{o.capitalize}"}
  end

  display_options.call("Choose the Entity", ["Facility", "OrganizationAffiliation", "Provider", "PractitionerRole"])
  entity_choice = gets.to_i
  return puts "Wrong argument" unless entity_choice <= 4

  display_options.call("Choose the Tenant process", ["All tenants", "One tenant", "Particular tenant source"])
  tenant_choice = gets.to_i
  return puts "Wrong argument" unless tenant_choice <= 3


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
      tenants.map { |tenant| puts "\n #{tenant}:"; instance_of_result_hash.call(tenant) }
    when 2
      display_options.call("Choose tenant", tenants)
      selected_tenant = tenants[gets.to_i - 1]
      instance_of_result_hash.call(selected_tenant)
    when 3
      puts "#{yellow}Enter source name with tenant (e.g., 'wahbe, wa-amerigroup'):#{reset}"
      tenant, source = gets.chomp.split(',').map(&:strip)
      instance_of_result_hash.call(tenant, source)
    else
      puts "Wrong argument"
    end
  end

  # passing the required resulthash to the validate 
  final_result_hash = validate_tenant_and_execute_resulthash.call(tenant_choice, instance_of_all_result_hashes[entity_choice - 1])
  puts JSON.generate(final_result_hash) if final_result_hash.present?
end