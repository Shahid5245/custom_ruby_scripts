
def fuzzy_org_match(org_aff_id)
  if (org_aff = OrganizationAffiliation.find_by(id: org_aff_id))
  elsif (fac = Facility.find_by(id: org_aff_id))
    org_aff = fac.organization_affiliation
  elsif (org = OrganizationAffiliation.find_by(stableId: org_aff_id))
    org_aff = org
  else
    return "Given id is not found in the database"
  end

  location = org_aff.locations.first
  npi_identifier = Utils::RecordIdentifiers.new.fetch_identifier(org_aff.facility, 'NPI')[0]
  dea_number_identifier = Utils::RecordIdentifiers.new.fetch_identifier_value(org_aff.facility.identifier, 'DEA')[0]
  license_number_identifier = Utils::RecordIdentifiers.new.split_license_value(org_aff.facility.identifier, 'LN')
  license_state_identifier = license_number_identifier.present? ? Utils::RecordIdentifiers.new.fetch_license_state_org(org_aff) : ''
  tax_number_identifier = Utils::RecordIdentifiers.new.fetch_identifier_value(org_aff.facility.identifier, 'TAX')
  medicare_number_identifier = Utils::RecordIdentifiers.new.fetch_identifier_value(org_aff.facility.identifier, 'MCR')
  medicaid_number_identifier = Utils::RecordIdentifiers.new.fetch_identifier_value(org_aff.facility.identifier, 'MCD')
  location = location.present? ? location : nil
  formated_name, formated_alias = Utils::Property.format_name_and_alias(org_aff.facility)

  template_search_params = {
    "id": 'search-template-org',
    "params": {
      'size': 10000,
      'min_score': Utils::Property.new.fetch_template_min_score('clarity_template_score', 'facility_score'),
      'npi': (npi_identifier.present? ? npi_identifier : ''),
      'name': (org_aff.facility.name rescue ''),
      'name_exact': (formated_name rescue ''),
      'alias': ((org_aff.facility.alias.present? ? org_aff.facility.alias : '') rescue ''),
      'alias_exact': (formated_alias rescue ''),
      's_hash': (location.stableId rescue ''),
      's_state': (location.address[1]['final_address']['state'] rescue ''),
      's_city': (location.address[1]['final_address']['city'] rescue ''),
      's_zip': (location.address[1]['final_address']['postalCode'] rescue ''),
      's_streetname': (location.address[1]['final_address']['streetName'] rescue ''),
      's_streetnumber': (location.address[1]['final_address']['primaryNumber'] rescue ''),
      's_secondarydesignator': (location.address[1]['final_address']['secondaryDesignator'] rescue ''),
      's_secondarynumber': (location.address[1]['final_address']['secondaryNumber'] rescue ''),
      'dea_number': ((dea_number_identifier.present? ? dea_number_identifier : '') rescue ''),
      'license_state': (license_state_identifier.present? ? license_state_identifier : ''),
      'name_tokens': ((formated_name.present? ? Nppes::Utility.generate_tokens(formated_name) : '') rescue ''),
      'alias_tokens': ((formated_alias.present? ? Nppes::Utility.generate_tokens(formated_alias) : '') rescue ''),
      'license_number_tokens': ((license_number_identifier.present? ? Utils::RecordIdentifiers.new.generate_tokens(license_number_identifier) : '') rescue ''),
      'tax_number_tokens': ((tax_number_identifier.present? ? Utils::RecordIdentifiers.new.generate_tokens(tax_number_identifier) : '') rescue ''),
      'medicare_number_tokens': ((medicare_number_identifier.present? ? Utils::RecordIdentifiers.new.generate_tokens(medicare_number_identifier) : '') rescue ''),
      'medicaid_number_tokens': ((medicaid_number_identifier.present? ? Utils::RecordIdentifiers.new.generate_tokens(medicaid_number_identifier) : '') rescue '')
    }
  }

  res = ELASTICSEARCH_CLIENT.search_template(index: ENV['ORGANIZATION_AFFILIATION_MASTER_INDEX'], body: template_search_params)
  json_template = template_search_params.to_json
  puts json_template
  {"matched_es_count"=> res["hits"]["total"]["value"]}
end

#-------------------------------------------------------------------------------------

def fuzzy_pr_match(pr_role_id)
  if (pr_role = PractitionerRole.find_by(id: pr_role_id))
    practitioner = pr_role.practitioner
  elsif (pr = Practitioner.find_by(id: pr_role_id))
    practitioner = pr
  elsif (pr_st = Practitioner.find_by(stableId: pr_role_id))
    practitioner = pr_st
  else
    return "Given id is not found in the database"
  end

  location = practitioner.practitioner_role.locations.first
  ln_identifier = ''
  stable_ids = practitioner.stableId
  npi_identifier = Utils::RecordIdentifiers.new.fetch_identifier(practitioner, 'NPI')[0]
  dea_number_identifier = Utils::RecordIdentifiers.new.fetch_identifier_value(practitioner.identifier, 'DEA')[0]
  license_number_identifier = Utils::RecordIdentifiers.new.split_license_value(practitioner.identifier, 'LN')
  license_state_identifier = license_number_identifier.present? ? Utils::RecordIdentifiers.new.fetch_license_state_pr(practitioner) : ''
  tax_number_identifier = Utils::RecordIdentifiers.new.fetch_identifier_value(practitioner.identifier, 'TAX')
  medicare_number_identifier = Utils::RecordIdentifiers.new.fetch_identifier_value(practitioner.identifier, 'MCR')
  medicaid_number_identifier = Utils::RecordIdentifiers.new.fetch_identifier_value(practitioner.identifier, 'MCD')

  template_search_params = {
    "id": 'search-template',
    "params": {
      'size': 10000,
      'min_score': Utils::Property.new.fetch_template_min_score('clarity_template_score', 'provider_score'),
      'npi': (npi_identifier.present? ? npi_identifier : ''),
      'firstname': (practitioner.name[0]['given'].join(', ') rescue ''),
      'lastname': (practitioner.name[0]['family'] rescue ''),
      'middlename': (practitioner.name[0]['middle'] rescue ''),
      'fullname': (practitioner.name[0]['text'] rescue ''),
      's_state': (location.address[1]['final_address']['state'] rescue ''),
      's_city': (location.address[1]['final_address']['city'] rescue ''),
      's_zip': (location.address[1]['final_address']['postalCode'] rescue ''),
      's_streetname': (location.address[1]['final_address']['streetName'] rescue ''),
      's_streetnumber': (location.address[1]['final_address']['primaryNumber'] rescue ''),
      's_secondarydesignator': (location.address[1]['final_address']['secondaryDesignator'] rescue ''),
      's_secondarynumber': (location.address[1]['final_address']['secondaryNumber'] rescue ''),
      'taxonomylicense': ln_identifier,
      'dea_number': ((dea_number_identifier.present? ? dea_number_identifier : '') rescue ''),
      'license_state': (license_state_identifier.present? ? license_state_identifier : ''),
      'license_number_tokens': ((license_number_identifier.present? ? Utils::RecordIdentifiers.new.generate_tokens(license_number_identifier) : '') rescue ''),
      'tax_number_tokens': ((tax_number_identifier.present? ? Utils::RecordIdentifiers.new.generate_tokens(tax_number_identifier) : '') rescue ''),
      'medicare_number_tokens': ((medicare_number_identifier.present? ? Utils::RecordIdentifiers.new.generate_tokens(medicare_number_identifier) : '') rescue ''),
      'medicaid_number_tokens': ((medicaid_number_identifier.present? ? Utils::RecordIdentifiers.new.generate_tokens(medicaid_number_identifier) : '') rescue ''),
      'birth_date': (practitioner.birthDate rescue '')
      }
  }

  res = ELASTICSEARCH_CLIENT.search_template(index: ENV['PRACTITIONER_ROLE_MASTER_INDEX'], body: template_search_params)
  json_template = template_search_params.to_json
  puts json_template
  {"matched_es_count"=> res["hits"]["total"]["value"]}
end

#-------------------------------------------------------------------------------------

def create_all_indexes
  es1_index, es2_index = ChewyClientIndex, ElasticClientIndex
  index_name_and_class = {
    "es_1" => {
      "practitioner_role_" => PractitionerRolesSingleDoc, "provider_" => PractitionerRolesTenantV3,
      "organization_affiliation_" => OrganizationAffiliationSingleDoc, "facility_" => OrganizationAffiliationTenantV3,
      "practice_" => OrganizationAffiliationTenantV3, "facility_by_location_" => FacilityByLocationIndex, "nppes_master" => NppesMasterIndex
    },
    "es_2" => {
      "organization_affiliation" => :OrganizationAffiliationIndex, "practitioner_roles" => :PractitionerRolesIndex, "standardized_address" => :StandardizedAddressIndex,
      "organization_affiliation_provider_group" => :OrgProviderGroupIndex, "practitioner_role_provider_group" => :PrProviderGroupIndex
    }
  }

  index_name_and_class["es_2"].each do |index_name, klass|
    unless es2_index.exists(index: index_name)
      ElasticIndex::const_get(klass).create
    end
  end

  AllTenants.each do |each_tenant|
    index_name_and_class["es_1"].each do |index_prefix, klass|
      index_name = index_prefix == "nppes_master" ?  index_prefix :  index_prefix + each_tenant + "_rw"
      unless es1_index.exists(index: index_name)
        klass.index_name(index_name)
        klass.create!
      end
      es1_index.put_alias index: index_name, name: index_name[0..-4] unless %w[nppes_master facility_by_location_].any?(index_prefix)
    end
  end
end
#-------------------------------------------------------------------------------------

def get_time_taken(meta_detail)
  meta_detail_id = meta_detail.id
  pr_count = PractitionerRole.where(meta_detail_id: meta_detail_id).count
  ingest_seconds = (PractitionerRole.where(meta_detail_id: meta_detail_id).order('updated_at').last.updated_at.in_time_zone('Asia/Kolkata')) - (PractitionerRole.where(meta_detail_id: meta_detail_id).order('updated_at').first.updated_at.in_time_zone('Asia/Kolkata')) rescue "Processing not completed"
  cluster_seconds = (StatLog.where(meta_detail_id: meta_detail_id, ingested: true).order('ingested_at').last.ingested_at.in_time_zone('Asia/Kolkata')) - (StatLog.where(meta_detail_id: meta_detail_id, ingested: true).order('ingested_at').first.ingested_at.in_time_zone('Asia/Kolkata')) rescue "Processing not completed"
  index_seconds = (StatLog.where(meta_detail_id: meta_detail_id, indexed: true).order('indexed_at').last.indexed_at.in_time_zone('Asia/Kolkata')) - (StatLog.where(meta_detail_id: meta_detail_id, indexed: true).order('indexed_at').first.indexed_at.in_time_zone('Asia/Kolkata')) rescue "Processing not completed"
  overall_seconds = 0.0
  overall_seconds += ingest_seconds if ingest_seconds.is_a?(Float)
  overall_seconds += cluster_seconds if cluster_seconds.is_a?(Float)
  overall_seconds += index_seconds if index_seconds.is_a?(Float)

  puts "Souce name                : #{meta_detail.source.name}"
  puts "Total records count       : #{pr_count}/#{meta_detail.total_records}"
  puts "Time taken for Ingestion  : #{(ingest_seconds.to_f/60.00).round(1)} min"
  puts "Time taken for Clustering : #{cluster_seconds.is_a?(Float) ? (cluster_seconds.to_f/60.00).round(1) : cluster_seconds} min"
  puts "Time taken for Indexing   : #{index_seconds.is_a?(Float) ? (index_seconds.to_f/60.00).round(1) : index_seconds} min"
  puts "Overall time taken        : #{(overall_seconds.to_f/60.00).round(1)} min"
end

#-------------------------------------------------------------------------------------

def send_indexing
  Stat::StatLogCheck.start_send_to_indexing_queue(MetaDetail.order(:created_at).last)
end

#-------------------------------------------------------------------------------------

def send_stat_log
  Stat::StatLogCheck.send_stat_logs(MetaDetail.order(:created_at).last)
end

#-------------------------------------------------------------------------------------

def latest_meta_detail
  count = block_given? ? yield : 1
  MetaDetail.order(:created_at).last(count)
end

def pluck_st_id(db_table_name)
  db_table_name.pluck(:stableId)
end

def pluck_rowhash(db_table_name)
  db_table_name.pluck(:rowHash)
end

def order(db_table_name)
  db_table_name.order(:created_at)
end

def latest(db_table_name)
  count = block_given? ? yield : 1
  db_table_name.order(:created_at).last(count)
end

def group_st_id(db_table_name)
  column_name, count = block_given? ? yield : :stableId
  db_table_name.group(column_name).order("count_all asc").count
end

def fetch_location(root_table_id)
  if (pr_obj = Practitioner.find_by(id: root_table_id))
    pr_obj.practitioner_role.locations
  elsif (pr_role_obj = PractitionerRole.find_by(id: root_table_id))
    pr_role_obj.locations
  elsif (org_obj = OrganizationAffiliation.find_by(id: root_table_id))
    org_obj.locations
  elsif (fac_obj = Facility.find_by(id: root_table_id))
    fac_obj.organization_affiliation.locations
  end
end


#-------------------------------------------------------------------------------------

def reset_api
  if Rails.env.development?
    response = HTTParty.post('http://localhost:3000/api/v1/reset')
  end
  puts response.body
end

#-------------------------------------------------------------------------------------

def consume_api(json)
  if Rails.env.development?
    response = HTTParty.post('http://localhost:3000/api/v1/consume', {body: JSON.generate(json)})
  end
  response_body = JSON.parse(response)
end

#-------------------------------------------------------------------------------------

def create_trigger_message
  DisplayOptions.call("Choose Entity", ['Facility', 'Provider', 'Nppes'])
  entity = gets.to_i
  if entity == 1
    source, profile, batchId = 'wa-amerigroup', OrgAffProfile, "facility:#{SecureRandom.uuid}"
  elsif entity == 2
    source, profile, batchId = 'wa-amerigroup', PrRoleProfile, "provider:#{SecureRandom.uuid}"
  elsif entity == 3
    source, profile, batchId = 'us-nppes', NppesProfile, "nppes:#{SecureRandom.uuid}"
  else
    return
  end

  DisplayOptions.call("Enter Filepath")
  filepath = gets().chomp

  triger_message = {
    "meta"=> { "SourceCode"=> source, "SourceFilePath"=> filepath, "profile"=> profile, "batchId"=> batchId },
    "processRules"=> { "batchSize"=> 1000, "ingestionMode"=> "delta", "forceReMatchOnDupes"=> true }
  }

  DisplayOptions.call("Choose the Trigger message type", ["Create trigger message", "Modify trigger message"])
  trig_mesg_type = Integer(gets())
  if trig_mesg_type == 2
    DisplayOptions.call("Choose the Process type", ["Source", "Injestionmode"])
    modify = Integer(gets())
    if modify == 1
      DisplayOptions.call("Enter Source name")
      source = gets().chomp
      triger_message["meta"]["SourceCode"] = source
    elsif modify == 2
      DisplayOptions.call("Choose the Injestionmode type", ["delta", "overwrite", "retirebatch"])
      injestion_mode = Integer(gets())
      if injestion_mode == 2
        triger_message["processRules"]["ingestionMode"] = 'overwrite'
      elsif injestion_mode == 3
        triger_message["processRules"]["ingestionMode"] = 'batchswap'
        triger_message["processRules"]["retireBatchId"] = [batchId]
      end
    end
  end

  puts triger_message.to_json
end

#-------------------------------------------------------------------------------------

def create_tenant_configs
  if Rails.env.development?
    load '/home/shahid/Documents/Clarity_project_files/Query_&_Script/5. Scripts/create_data_source_and_tenant.rb'
  end
end;1

#-------------------------------------------------------------------------------------

def fetch_matching_st_ids_records_org(stableids='', skip_1_count=false)
  stable_ids = stableids.present? ? [stableids].flatten : OrganizationAffiliation.pluck(:stableId).uniq
  result_hash = {}

  stable_ids.each_with_index do |each_st_id, index|
    org_objects = OrganizationAffiliation.where(stableId: each_st_id)
    next unless org_objects.present?
    next if org_objects.count == 1 && skip_1_count == true

    result_hash[each_st_id] ||= {"stableId_count" => '', "name" => [], "locaton_text" => [], "NPI" => [] }
    result_hash[each_st_id]['stableId_count'] =  org_objects.count
    org_objects.each do |each_obj|
      result_hash[each_st_id]['name'] << each_obj.facility.name
      result_hash[each_st_id]['locaton_text'] << each_obj.locations.last.address[1]['final_address']['text']
      result_hash[each_st_id]['NPI'] << Utils::RecordIdentifiers.new.fetch_identifier(each_obj.facility, 'NPI')[0]
    end
    result_hash[each_st_id]['name'].uniq!&.sort!
    result_hash[each_st_id]['locaton_text'].uniq!&.sort!
    result_hash[each_st_id]['NPI'].uniq!&.sort!
    print "\e[2K\rCheck cluster matching:::#{index + 1}:::#{stable_ids.size}"
  end;1
  if result_hash.present?
    sorted_hash = result_hash.sort_by { |_, value| value["name"].first }.to_h
    sorted_hash = { "total_uniq_st_count" => stable_ids.count }.merge(sorted_hash)
  else
    result_hash['error'] => "stableid not present"
  end
  puts sorted_hash.to_json
end;1

#-------------------------------------------------------------------------------------

def fetch_matching_st_ids_records_pr(stableids='', skip_1_count=false)
  stable_ids = stableids.present? ? [stableids].flatten : Practitioner.pluck(:stableId).uniq
  result_hash = {}
  stable_ids.each_with_index do |each_st_id, index|
    pr_objects = Practitioner.where(stableId: each_st_id)
    next unless pr_objects.present? 
    next if pr_objects.count == 1 && skip_1_count == true

    result_hash[each_st_id] ||= {"stableId_count" => '', "name" => [], "locaton_text" => [], "NPI" => [] }
    result_hash[each_st_id]['stableId_count'] =  pr_objects.count
    pr_objects.each do |each_obj|
      result_hash[each_st_id]['name'] << each_obj.name[0]['text']
      result_hash[each_st_id]['locaton_text'] << each_obj.practitioner_role.locations.last.address[1]['final_address']['text'] rescue nil
      result_hash[each_st_id]['NPI'] << Utils::RecordIdentifiers.new.fetch_identifier(each_obj, 'NPI')[0]
    end
    result_hash[each_st_id]['name'].uniq!&.sort!
    result_hash[each_st_id]['locaton_text'].uniq!&.sort!
    result_hash[each_st_id]['NPI']&.uniq!&.sort! rescue nil
    print "\e[2K\rstable_ids:::#{index + 1}:::#{stable_ids.size}"
  end;1
  if result_hash.present?
    sorted_hash = result_hash.sort_by { |_, value| value["name"].first }.to_h
    result_hash = { "total_uniq_st_count" => stable_ids.count }.merge(sorted_hash)
  else
    result_hash['error'] => "stableid not present"
  end
  puts result_hash.to_json
end;1


#-------------------------------------------------------------------------------------
