  def latest_meta_detail
    MetaDetail.order(:created_at).last
  end

#-------------------------------------------------------------------------------------

def fuzzy_org_match(org_aff_id)
  org_aff  = OrganizationAffiliation.find org_aff_id
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


def get_delete_org_ids(root_ids_to_be_deleted, tenant )
  # tenant =tenant['key']
  tenant
  root_ids_to_be_deleteds = root_ids_to_be_deleted
  org_aff_data={}
  root_ids_to_be_deleteds.each do |org_aff_id|
    OrganizationAffiliationSingleDoc.index_name("organization_affiliation_#{tenant}_rw")
    es_org = OrganizationAffiliationSingleDoc.find(org_aff_id) rescue nil
    stable_id = es_org.present? ? es_org.as_json['_data']['_source']['stableId'] : nil
    stable_id = OrganizationAffiliation.find_by(id: org_aff_id)&.stableId unless stable_id.present?
    org_ids_for_delete = [org_aff_id]
    if stable_id.present?
      org_aff_st_ids = OrganizationAffiliation.where(stableId: stable_id)

      org_aff_data[org_aff_id] = {} unless org_aff_data[org_aff_id]

      source_ids = org_aff_st_ids.pluck(:source_id).uniq
      source_names = Source.where(id: source_ids).pluck(:name)

      org_aff_data[org_aff_id]['source'] = {source_ids => source_names}

      tenant_source_names = Utils::Property.new.get_source_by_tenant(tenant, false)
      tenant_include_source_names = Utils::Property.new.get_include_source_by_tenant(tenant)
      tenant_include_source_ids = Source.where(name: tenant_include_source_names).pluck(:id)
      if tenant_source_names.present? && source_names.present? && tenant_source_names.exclude?('all')
        unless source_names.any? { |source| tenant_source_names.include?(source) }
          org_ids_for_delete += org_aff_st_ids.where(source_id: tenant_include_source_ids).pluck(:id)
        end
      end
      org_aff_data[org_aff_id]['org_ids_for_delete'] = org_ids_for_delete
    end
  end
  org_aff_data
end

#-------------------------------------------------------------------------------------

def send_indexing
  Stat::StatLogCheck.start_send_to_indexing_queue(MetaDetail.order(:created_at).last)
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
