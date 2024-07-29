require_relative 'result_hash_v6_script'

def initial_result_hash
  @yellow = "\033[33m"
  @reset = "\033[0m"
  @tenants = %w[wahbe centerlight riverspring honestmedical mddoh]
  final_result_hash = ""


  puts "\n#{@yellow}Choose the Entity:#{@reset}"
  puts "1.Facility"
  puts "2.OrganizationAffiliation"
  puts "3.Provider"
  puts "4.PractitionerRole"

  entity = gets.to_i

  def choosing_tenants
    puts "\n#{@yellow}Proceed the tenants:#{@reset}"
    puts "1.All tenants"
    puts "2.One tenant"
    puts "3.Praticular tenant source"
    tenant = gets.to_i
  end

  def print_tenants
    @tenants.each_with_index do |each_tenant, index|
      puts "#{index+1}.#{each_tenant.capitalize}"
    end 
  end

  def tenant_validation(tenant, class_of_result_hash)
    result_array = ""
    if tenant == 1
      result_array = []
      @tenants.each do |each_tent|
        puts "\n #{each_tent}:"
        result_array << class_of_result_hash.call(each_tent)
      end;1
    elsif tenant == 2
      puts "\n#{@yellow}Choose tenant#{@reset}"
      print_tenants
      tenant = gets.to_i - 1
      result_array =  class_of_result_hash.call(@tenants[tenant])
  
    elsif tenant == 3
      puts "#{@yellow}write a source name with tenant : ex:('wahbe', 'wa-amerigroup')#{@reset}"
      tenant, source = gets.chomp.split(",").map(&:strip)
      result_array =  class_of_result_hash.call(tenant, source)
    else
      return "Wrong argument"
    end
    result_array
  end

  
  if entity == 1 
    tenant = choosing_tenants
    class_of_result_hash = lambda do |tenant, source = nil|
      ResultHash.result_hash_for_cluster_doc_org(tenant, source)
    end
    final_result_hash = tenant_validation(tenant, class_of_result_hash)

  else
    puts "Wrong argument"
  end
  puts final_result_hash
end

# initial_result_hash