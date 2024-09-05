

def fuzzy_org_match(org_aff_id)
  org_aff  = OrganizationAffiliation.find_by(org_aff_id)
  return "OrganizationAffiliation record not found" unless org_aff.present?

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
      'min_score': ENV['CLARITY_FACILITY_TEMPLATE_MIN_SCORE'].to_i,
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
  return {"matched_es_count"=> res["hits"]["total"]["value"]}
end

#-------------------------------------------------------------------------------------

def fuzzy_pr_match(practitioner_id)
  practitioner = Practitioner.find_by(id:practitioner_id) 
  unless practitioner.present?
    pr_role_obj = PractitionerRole.find practitioner_id
    practitioner = pr_role_obj.practitioner
    return "Practitioner record not found" unless practitioner.present?
  end

  location = practitioner.practitioner_role.locations.first
  pr_name = practitioner.name.first
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
      'min_score': ENV['CLARITY_PROVIDER_TEMPLATE_MIN_SCORE'].to_i,
      'npi': (npi_identifier.present? ? npi_identifier : ''),
      'firstname': (pr_name['given'].join(', ').gsub("'",'') rescue ''),
      'lastname': (pr_name['family'].gsub("'",'') rescue ''),
      'middlename': (pr_name['middle'].gsub("'",'') rescue ''),
      'fullname': (pr_name['text'].gsub("'",'') rescue ''),
      's_state': (location.address[1]['final_address']['state'] rescue ''),
      's_city': (location.address[1]['final_address']['city'] rescue ''),
      's_zip': (location.address[1]['final_address']['postalCode'] rescue ''),
      's_streetname': (location.address[1]['final_address']['streetName'] rescue ''),
      's_streetnumber': (location.address[1]['final_address']['primaryNumber'] rescue ''),
      's_secondarydesignator': (location.address[1]['final_address']['secondaryDesignator'] rescue ''),
      's_secondarynumber': (location.address[1]['final_address']['secondaryNumber'] rescue ''),
      'taxonomylicense': '',
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
  return {"matched_es_count"=> res["hits"]["total"]["value"]}
end

#-------------------------------------------------------------------------------------

def create_all_indexes(all=nil)
  es1_index = Chewy.client.indices
  es2_index = ELASTICSEARCH_CLIENT.indices
  tenant_names = Utils::Property.new.get_tenants.map { |i| i['key'] }.reject{ |i| i == "qa-tenant" && all==nil  }

  unless es2_index.exists(index: 'organization_affiliation')
    ElasticIndex::OrganizationAffiliationIndex.create
  end

  unless es2_index.exists(index: 'practitioner_roles')
    ElasticIndex::PractitionerRolesIndex.create
  end

  unless es2_index.exists(index: 'organization_affiliation_provider_group')
    ElasticIndex::OrgProviderGroupIndex.create 
  end

  unless es2_index.exists(index: 'practitioner_role_provider_group')
    ElasticIndex::PrProviderGroupIndex.create 
  end

  tenant_names.each do |each_tenant|
    practitioner_role_index_name = "practitioner_role_#{each_tenant}_rw"
    unless es1_index.exists(index: practitioner_role_index_name)
      PractitionerRolesSingleDoc.index_name(practitioner_role_index_name)
      PractitionerRolesSingleDoc.create!
    end

    provider_index_name = "provider_#{each_tenant}_rw"
    unless es1_index.exists(index: provider_index_name)
      PractitionerRolesTenantV3.index_name(provider_index_name)
      PractitionerRolesTenantV3.create!
    end

    organization_affiliation_index_name = "organization_affiliation_#{each_tenant}_rw"
    unless es1_index.exists(index: organization_affiliation_index_name)
      OrganizationAffiliationSingleDoc.index_name(organization_affiliation_index_name)
      OrganizationAffiliationSingleDoc.create!
    end

    facility_index_name = "facility_#{each_tenant}_rw"
    unless es1_index.exists(index: facility_index_name)
      OrganizationAffiliationTenantV3.index_name(facility_index_name)
      OrganizationAffiliationTenantV3.create!
    end

    practice_index_name = "practice_#{each_tenant}_rw"
    unless es1_index.exists(index: practice_index_name)
      OrganizationAffiliationTenantV3.index_name(practice_index_name)
      OrganizationAffiliationTenantV3.create!
    end
  end

  nppes_index_name = "nppes_master"
  unless es1_index.exists(index: nppes_index_name)
    NppesMasterIndex.index_name(nppes_index_name)
    NppesMasterIndex.create!
  end

  if all
    facility_by_location_index_name = "facility_by_location_rw"
    unless es1_index.exists(index: facility_by_location_index_name)
      FacilityByLocationIndex.index_name(facility_by_location_index_name)
      FacilityByLocationIndex.create!
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
  if pr_obj = Practitioner.find_by(id:root_table_id)
    pr_obj.practitioner_role.locations
  elsif pr_role_obj = PractitionerRole.find_by(id:root_table_id)
    pr_role_obj.locations
  elsif org_obj = OrganizationAffiliation.find_by(id:root_table_id)
    org_obj.locations
  elsif fac_obj = Facility.find_by(id:root_table_id)
    fac_obj.organization_affiliation.locations
  end
end


#-------------------------------------------------------------------------------------
