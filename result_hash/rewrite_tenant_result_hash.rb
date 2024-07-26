# Rewrite tenant result_hash

# --------------------------------------------
# Provider

def provider_result_hash(tenant, input_source_name = nil)
  return 'Invalid Tenant' unless Utils::Property.new.get_all_active_tenant_key.include?(tenant)

  print_all_batch_id = false
  rm_proper_source_from_result = false

  source_names = input_source_name.present? ? [input_source_name] : Utils::Property.new.get_source_by_tenant(tenant, true)
  include_source_names = Utils::Property.new.get_include_source_by_tenant(tenant)
  all_source_names = Utils::Property.new.get_source_by_tenant(tenant, true)

  must_source_names = all_source_names - include_source_names
  must_source_ids = Source.where(name: must_source_names).ids
  must_source_ids_str = must_source_ids.map { |value| "'#{value}'" }.join(',')

  total_count = source_names.count
  count = 1
  PractitionerRolesTenantV3.index_name "provider_#{tenant}_rw"
  result_hash = { entity: 'Provider', tenant:, index_name: "provider_#{tenant}_rw", ENV: Rails.env.to_s }.as_json
  location_count = PractitionerRolesTenantV3.query({ "nested": { "path": 'practitionerRole', "query": { "bool": { "must_not": [{ "nested": { "path": 'practitionerRole.location', "query": { "exists": { "field": 'practitionerRole.location' } } } }] } } } }).count
  result_hash['location'] = { location_must_not_exist: location_count }.as_json
  source_names.each do |source_name|
    next if %w[riverspring lo-cmsproviderofservices].include?(source_name)

    puts "#{count}:::#{total_count}:::#{source_name}"
    count += 1
    source = Source.where(name: source_name).first
    next unless source.present?

    profile = 'http://hl7.org/fhir/us/core/StructureDefinition/us-core-practitionerrole'
    meta_details = MetaDetail.where(source_id: source.id, profile: profile).order('created_at')
    next unless meta_details.present?

    latest_meta_detail = MetaDetail.where(source_id: source.id, profile: profile).order('created_at').last

    uniq_stable_id_db_count = if include_source_names.include?(source_name)
                                Practitioner.connection.execute("SELECT COUNT(DISTINCT p1.\"stableId\")
                                                            FROM practitioners p1
                                                            JOIN practitioners p2 ON p1.\"stableId\" = p2.\"stableId\"
                                                            WHERE p1.source_id IN (#{must_source_ids_str})
                                                              AND p2.source_id = '#{source.id}'").to_a[0]['count']
                              else
                                Practitioner.connection.execute("SELECT COUNT(DISTINCT p.\"stableId\")
                                                            FROM practitioners p
                                                            WHERE p.source_id = '#{source.id}'").to_a[0]['count']
                              end

    updated_dates = meta_details.collect { |i| [i.batchId, i.index_time.strftime('%Y-%m-%dT%H:%M:%S.%LZ')] }
    result_hash[source_name] = {} unless result_hash[source_name].present?

    result_hash[source_name]['total_records_in_db'] = Practitioner.where(source_id: source.id).count
    updated_dates.each do |batch_id, updated|
      res_count = PractitionerRolesTenantV3.query({ "bool": { "must": [{ "nested": { "path": 'practitionerRole', "query": { "match": { "practitionerRole.meta.source": { "query": "#{source_name}", "operator": 'and' } } } } }, { "nested": { "path": 'practitionerRole', "query": { "match": { "practitionerRole.meta.updated": { "query": "#{updated}", "operator": 'and' } } } } }] } }).count
      res_meta = MetaDetail.where(batchId: batch_id).first
      plan_year_json = Practitioner.connection.execute("SELECT p.period
                            FROM practitioner_roles p
                            WHERE p.meta_detail_id = '#{res_meta.id}' LIMIT 1").to_a
      plan_year = JSON.parse(plan_year_json[0]['period'])['planYear'] rescue ''
      year = plan_year.present? ? "#{plan_year}::::" : ''
      result_hash[source_name]["#{year}#{batch_id}::::#{updated}"] = res_count if (res_count > 0 || print_all_batch_id)
    end
    result_hash[source_name]['uniq_st_id_in_DB'] = uniq_stable_id_db_count
    uniq_st_id_es = PractitionerRolesTenantV3.query({ "nested": { "path": 'practitionerRole', "query": { "match": { "practitionerRole.meta.source": { "query": "#{source_name}", "operator": 'and' } } } } })
    result_hash[source_name]['uniq_st_id_in_ES'] = uniq_st_id_es.count
    result_hash[source_name]['ingestionMode'] = latest_meta_detail.processRules&.dig('ingestionMode')
    result_hash[source_name]['difference'] = uniq_stable_id_db_count - uniq_st_id_es.count
    result_hash[source_name]['processed'] = latest_meta_detail.processed
    result_hash.delete(source_name) if rm_proper_source_from_result && result_hash[source_name]['difference'] == 0 && result_hash[source_name]['processed'] == true
  end; 1
  result_hash
end

# result_hash = provider_result_hash('honestmedical', 'hmg-honestpractitioner')
# puts JSON.generate(result_hash)

# result_array = []
# %w[wahbe centerlight riverspring honestmedical mddoh].each do |t|
#   puts "\n #{t}:"
#   result_array << provider_result_hash(t)
# end; 1
# puts JSON.generate(result_array)

# --------------------------------------------
# Facility

def facility_result_hash(tenant, input_source_name = nil)
  return 'Invalid Tenant' unless Utils::Property.new.get_all_active_tenant_key.include?(tenant)

  print_all_batch_id = false
  rm_proper_source_from_result = false

  source_names = input_source_name.present? ? [input_source_name] : Utils::Property.new.get_source_by_tenant(tenant, true)
  include_source_names = Utils::Property.new.get_include_source_by_tenant(tenant)
  all_source_names = Utils::Property.new.get_source_by_tenant(tenant, true)

  must_source_names = all_source_names - include_source_names
  must_source_ids = Source.where(name: must_source_names).ids
  must_source_ids_str = must_source_ids.map { |value| "'#{value}'" }.join(',')

  total_count = source_names.count
  count = 1
  OrganizationAffiliationTenantV3.index_name "facility_#{tenant}_rw"
  result_hash = { entity: 'Facility', tenant:, index_name: "facility_#{tenant}_rw", ENV: Rails.env.to_s }.as_json
  location_count = OrganizationAffiliationTenantV3.query({ "nested": { "path": 'organizationAffiliation', "query": { "bool": { "must_not": [{ "nested": { "path": 'organizationAffiliation.location', "query": { "exists": { "field": 'organizationAffiliation.location' } } } }] } } } }).count
  result_hash['location'] = { location_must_not_exist: location_count }.as_json
  source_names.each do |source_name|
    next if %w[riverspring lo-cmsproviderofservices].include?(source_name)

    puts "#{count}:::#{total_count}:::#{source_name}"
    count += 1
    source = Source.where(name: source_name).first
    next unless source.present?

    profile = 'http://hl7.org/fhir/us/core/STU5.0.1/StructureDefinition/us-core-OrganizationAffiliation'
    meta_details = MetaDetail.where(source_id: source.id, profile: profile).order('created_at')
    next unless meta_details.present?

    latest_meta_detail = MetaDetail.where(source_id: source.id, profile: profile).order('created_at').last

    uniq_stable_id_db_count = if include_source_names.include?(source_name)
                                OrganizationAffiliation.connection.execute("SELECT COUNT(DISTINCT p1.\"stableId\")
                                                            FROM organization_affiliations p1
                                                            JOIN organization_affiliations p2 ON p1.\"stableId\" = p2.\"stableId\"
                                                            WHERE p1.source_id IN (#{must_source_ids_str})
                                                              AND p2.source_id = '#{source.id}'").to_a[0]['count']
                              else
                                OrganizationAffiliation.connection.execute("SELECT COUNT(DISTINCT p.\"stableId\")
                                                            FROM organization_affiliations p
                                                            WHERE p.source_id = '#{source.id}'").to_a[0]['count']
                              end

    updated_dates = meta_details.collect { |i| [i.batchId, i.index_time.strftime('%Y-%m-%dT%H:%M:%S.%LZ')] }
    result_hash[source_name] = {} unless result_hash[source_name].present?

    result_hash[source_name]['total_records_in_db'] = OrganizationAffiliation.connection.execute("SELECT COUNT(*)
                                                                FROM organization_affiliations
                                                                WHERE source_id = '#{source.id}'
                                                                ").to_a[0]['count']

    updated_dates.each do |batch_id, updated|
      res_count = OrganizationAffiliationTenantV3.query({ "bool": { "must": [{ "nested": { "path": "organizationAffiliation", "query": { "match": { "organizationAffiliation.meta.source": { "query": "#{source_name}", "operator": 'and' } } } } }, { "nested": { "path": 'organizationAffiliation', "query": { "match": { "organizationAffiliation.meta.updated": { "query": "#{updated}", "operator": 'and' } } } } }] } }).count
      res_meta = MetaDetail.where(batchId: batch_id).first
      plan_year_json = OrganizationAffiliation.connection.execute("SELECT p.period
                            FROM organization_affiliations p
                            WHERE p.meta_detail_id = '#{res_meta.id}' LIMIT 1").to_a
      plan_year = JSON.parse(plan_year_json[0]['period'])['planYear'] rescue ''
      year = plan_year.present? ? "#{plan_year}::::" : ''
      result_hash[source_name]["#{year}#{batch_id}::::#{updated}"] = res_count if (res_count > 0 || print_all_batch_id)
    end
    result_hash[source_name]['uniq_st_id_in_DB'] = uniq_stable_id_db_count
    uniq_st_id_es = OrganizationAffiliationTenantV3.query({ "nested": { "path": "organizationAffiliation", "query": { "match": { "organizationAffiliation.meta.source": { "query": "#{source_name}", "operator": 'and' } } } } })
    result_hash[source_name]['uniq_st_id_in_ES'] = uniq_st_id_es.count
    result_hash[source_name]['ingestionMode'] = latest_meta_detail.processRules&.dig('ingestionMode')
    result_hash[source_name]['difference'] = uniq_stable_id_db_count - uniq_st_id_es.count
    result_hash[source_name]['processed'] = latest_meta_detail.processed
    result_hash.delete(source_name) if rm_proper_source_from_result && result_hash[source_name]['difference'] == 0 && result_hash[source_name]['processed'] == true
  end; 1
  result_hash
end

# result_hash = facility_result_hash('wahbe', 'wa-cc-medicaid')
# puts JSON.generate(result_hash)

# result_array = []
# %w[wahbe centerlight riverspring honestmedical mddoh].each do |t|
#   puts "\n #{t}:"
#   result_array << facility_result_hash(t)
# end; 1
# puts JSON.generate(result_array)

# =================================================================================================
# -----------------------------------------------------
# Provider -> If the value of difference is not 0 , then run this

def rm_provider_old_data_es(tenant, input_source_name = nil)
  return 'Invalid Tenant' unless Utils::Property.new.get_all_active_tenant_key.include?(tenant)

  source_names = input_source_name.present? ? [input_source_name] : Utils::Property.new.get_source_by_tenant(tenant, true)
  include_source_names = Utils::Property.new.get_include_source_by_tenant(tenant)
  all_source_names = Utils::Property.new.get_source_by_tenant(tenant, true)

  must_source_names = all_source_names - include_source_names
  must_source_ids = Source.where(name: must_source_names).ids
  must_source_ids_str = must_source_ids.map { |value| "'#{value}'" }.join(',')

  total_count = source_names.count
  count = 1
  delete_report = {}
  source_names.each do |source_name|
    next if %w[riverspring lo-cmsproviderofservices].include?(source_name)

    puts "#{count}:::#{total_count}:::#{source_name}"
    count += 1
    source = Source.where(name: source_name).first
    next unless source.present?

    profile = 'http://hl7.org/fhir/us/core/StructureDefinition/us-core-practitionerrole'
    meta_detail = MetaDetail.where(source_id: source.id, profile: profile).order('created_at').last
    next unless meta_detail.present?

    PractitionerRolesTenantV3.index_name "provider_#{tenant}_rw"
    es_stable_ids = PractitionerRolesTenantV3.query({ "nested": { "path": 'practitionerRole', "query": { "match": { "practitionerRole.meta.source": { "query": "#{source_name}", "operator": 'and' } } } } }).pluck(:_id)

    if include_source_names.include?(source_name)
      db_stable_ids_res = Practitioner.connection.execute("SELECT DISTINCT p1.\"stableId\"
                                                            FROM practitioners p1
                                                            JOIN practitioners p2 ON p1.\"stableId\" = p2.\"stableId\"
                                                            WHERE p1.source_id IN (#{must_source_ids_str})
                                                              AND p2.source_id = '#{source.id}'").to_a
      db_stable_ids = db_stable_ids_res.map { |i| i['stableId'] }
    else
      db_stable_ids = Practitioner.where(source_id: source.id).pluck(:stableId).uniq
    end

    next if db_stable_ids.count == es_stable_ids.count

    pr_stable_ids = db_stable_ids.count > es_stable_ids.count ? db_stable_ids - es_stable_ids : es_stable_ids - db_stable_ids

    # next if db_stable_ids.count > es_stable_ids.count
    # pr_stable_ids = es_stable_ids - db_stable_ids

    total_st_id_count = pr_stable_ids.count
    st_id_count = 1

    deleted_count_arr = []
    updated_count_arr = []

    stable_ids_for_update = []
    stable_ids_for_delete = []
    pr_stable_ids.each do |es_st_id|
      puts "#{st_id_count}:::#{total_st_id_count}"
      st_id_count += 1
      if Helper::TenantUtils.send_provider_for_update?(es_st_id, tenant)
        stable_ids_for_update << es_st_id
        updated_count_arr << es_st_id
      else
        stable_ids_for_delete << es_st_id
        deleted_count_arr << es_st_id
      end
      if stable_ids_for_update.count == Utils::Property.new.fetch_indexing_batch_size('stable_id')
        Load::CommonMatchingRules.new.send_stable_ids_to_update(meta_detail, stable_ids_for_update)
        stable_ids_for_update = []
      end
      if stable_ids_for_delete.count == ENV['DELETE_BATCH_SIZE'].to_i
        Load::CommonMatchingRules.new.send_stable_ids_to_delete(meta_detail, stable_ids_for_delete)
        stable_ids_for_delete = []
      end
    end
    Load::CommonMatchingRules.new.send_stable_ids_to_update(meta_detail, stable_ids_for_update)
    Load::CommonMatchingRules.new.send_stable_ids_to_delete(meta_detail, stable_ids_for_delete)
    delete_report[source_name] = {} unless delete_report[source_name].present?
    delete_report[source_name]['total_diff'] = total_st_id_count
    delete_report[source_name]['total_send_for_update'] = updated_count_arr.count
    delete_report[source_name]['total_send_for_delete'] = deleted_count_arr.count
  end
  delete_report
end

# delete_report = rm_provider_old_data_es('honestmedical')
# delete_report = rm_provider_old_data_es('centerlight', 'us-nppes')
# puts JSON.generate(delete_report)

# -----------------------------------------------------
# Facility -> If the value of difference is not 0 , then run this

def rm_facility_old_data_es(tenant, input_source_name = nil)
  return 'Invalid Tenant' unless Utils::Property.new.get_all_active_tenant_key.include?(tenant)

  source_names = input_source_name.present? ? [input_source_name] : Utils::Property.new.get_source_by_tenant(tenant, true)
  include_source_names = Utils::Property.new.get_include_source_by_tenant(tenant)
  all_source_names = Utils::Property.new.get_source_by_tenant(tenant, true)

  must_source_names = all_source_names - include_source_names
  must_source_ids = Source.where(name: must_source_names).ids
  must_source_ids_str = must_source_ids.map { |value| "'#{value}'" }.join(',')

  total_count = source_names.count
  count = 1
  delete_report = {}
  source_names.each do |source_name|
    next if %w[riverspring lo-cmsproviderofservices].include?(source_name)

    puts "#{count}:::#{total_count}:::#{source_name}"
    count += 1
    source = Source.where(name: source_name).first
    next unless source.present?

    profile = 'http://hl7.org/fhir/us/core/STU5.0.1/StructureDefinition/us-core-OrganizationAffiliation'
    meta_detail = MetaDetail.where(source_id: source.id, profile: profile).order('created_at').last
    next unless meta_detail.present?

    OrganizationAffiliationTenantV3.index_name "facility_#{tenant}_rw"
    es_stable_ids = OrganizationAffiliationTenantV3.query({ "nested": { "path": 'organizationAffiliation', "query": { "match": { "organizationAffiliation.meta.source": { "query": "#{source_name}", "operator": 'and' } } } } }).pluck(:_id)

    if include_source_names.include?(source_name)
      db_stable_ids_res = OrganizationAffiliation.connection.execute("SELECT DISTINCT p1.\"stableId\"
                                                            FROM organization_affiliations p1
                                                            JOIN organization_affiliations p2 ON p1.\"stableId\" = p2.\"stableId\"
                                                            WHERE p1.source_id IN (#{must_source_ids_str})
                                                              AND p2.source_id = '#{source.id}'").to_a
      db_stable_ids = db_stable_ids_res.map { |i| i['stableId'] }
    else
      db_stable_ids = OrganizationAffiliation.where(source_id: source.id).pluck(:stableId).uniq
    end

    next if db_stable_ids.count == es_stable_ids.count

    org_stable_ids = db_stable_ids.count > es_stable_ids.count ? db_stable_ids - es_stable_ids : es_stable_ids - db_stable_ids

    # next if db_stable_ids.count > es_stable_ids.count
    # org_stable_ids = es_stable_ids - db_stable_ids

    total_st_id_count = org_stable_ids.count
    st_id_count = 1

    deleted_count_arr = []
    updated_count_arr = []

    stable_ids_for_update = []
    stable_ids_for_delete = []
    org_stable_ids.each do |es_st_id|
      puts "#{st_id_count}:::#{total_st_id_count}"
      st_id_count += 1
      if Helper::TenantUtils.send_facility_for_update?(es_st_id, tenant)
        stable_ids_for_update << es_st_id
        updated_count_arr << es_st_id
      else
        stable_ids_for_delete << es_st_id
        deleted_count_arr << es_st_id
      end
      if stable_ids_for_update.count == Utils::Property.new.fetch_indexing_batch_size('stable_id')
        Load::CommonMatchingRulesOrg.new.send_stable_ids_to_update(meta_detail, stable_ids_for_update)
        stable_ids_for_update = []
      end
      if stable_ids_for_delete.count == ENV['DELETE_BATCH_SIZE'].to_i
        Load::CommonMatchingRulesOrg.new.send_stable_ids_to_delete(meta_detail, stable_ids_for_delete)
        stable_ids_for_delete = []
      end
    end
    Load::CommonMatchingRulesOrg.new.send_stable_ids_to_update(meta_detail, stable_ids_for_update)
    Load::CommonMatchingRulesOrg.new.send_stable_ids_to_delete(meta_detail, stable_ids_for_delete)
    delete_report[source_name] = {} unless delete_report[source_name].present?
    delete_report[source_name]['total_diff'] = total_st_id_count
    delete_report[source_name]['total_send_for_update'] = updated_count_arr.count
    delete_report[source_name]['total_send_for_delete'] = deleted_count_arr.count
  end
  delete_report
end

# delete_report = rm_facility_old_data_es('centerlight')
# delete_report = rm_facility_old_data_es('wahbe', 'wa-cc-medicaid')
# puts JSON.generate(delete_report)

# -----------------------------------------------------






# =================================================================================================
# -----------------------------------------------------
# Facility send to tenant index based on correlationId STATLOG

source_name = 'md-medicaid'
source = Source.where(name: source_name).first
profile = 'http://hl7.org/fhir/us/core/STU5.0.1/StructureDefinition/us-core-OrganizationAffiliation'
meta_detail = MetaDetail.where(source_id: source.id, profile: profile).order('created_at').last
meta_detail

# meta_detail = MetaDetail.where(batchId: 'facility:95abf9d5-c53b-433e-ae8f-47420acf5728').first

LoadLog.where(meta_detail_id: meta_detail).where.not(item_type: 'JSON_PARSE').pluck(:item_type).tally

StatLog.where(meta_detail_id: meta_detail).count
OrganizationAffiliation.where(meta_detail_id: meta_detail).count

cor_ids = StatLog.where(meta_detail_id: meta_detail, processed: false).ids
org_stable_ids = OrganizationAffiliation.where(correlationId: cor_ids).pluck(:stableId).uniq

# org_stable_ids = OrganizationAffiliation.where(id: org_ids).pluck(:stableId).uniq

# LoadLog.where(meta_detail_id: meta_detail).where.not(item_type: 'JSON_PARSE').where(item_type: 'ORGL').last
# OrganizationAffiliation.where(stableId: 'CLA:32de554e-0681-4e7f-bd16-577848a14df9').count
# Facility.where(rowHash: OrganizationAffiliation.where(stableId: org_stable_ids).pluck(:rowHash)).pluck(:name).tally
# OrganizationAffiliation.where(meta_detail_id: meta_detail).count
# org_stable_ids = OrganizationAffiliation.where(meta_detail_id: meta_detail).pluck(:stableId).uniq

total_count = org_stable_ids.count
count = 1
facility_tenant_index_rk = Utils::Property.new.fetch_routing_key(meta_detail, 'facility_tenant_index')
org_aff_tenant_index_rk = Utils::Property.new.fetch_routing_key(meta_detail, 'organization_affiliation_tenant_index')

org_stable_ids.in_groups_of(25, false).each do |stable_ids|
  puts "#{count}:::#{total_count}"
  count += 1
  message = { meta_id: meta_detail.id, stable_ids: stable_ids, source_name: meta_detail.source.name }.as_json
  # Bulk::FacilityTenant.run(meta_detail, message)

  params = { routing_key: facility_tenant_index_rk, timestamp: Time.now.to_i }
  Notification.send_notification(message.as_json, params)

  # Bulk::OrganizationAffiliationTenant.run(meta_detail, message)
  params = { routing_key: org_aff_tenant_index_rk, timestamp: Time.now.to_i }
  Notification.send_notification(message.as_json, params)
end; 1

# -----------------------------------------------------
# Provider send to tenant index based on correlationId STATLOG

source_name = 'md-medicaid'
source = Source.where(name: source_name).first
profile = 'http://hl7.org/fhir/us/core/StructureDefinition/us-core-practitionerrole'
meta_detail = MetaDetail.where(source_id: source.id, profile: profile).order('created_at').last
meta_detail

# meta_detail = MetaDetail.where(batchId: 'provider:44d72520-cd4f-4de5-91ac-905c3998cf7d').first

LoadLog.where(meta_detail_id: meta_detail).where.not(item_type: 'JSON_PARSE').pluck(:item_type).tally

cor_ids = StatLog.where(meta_detail_id: meta_detail, processed: false).ids
pr_stable_ids = Practitioner.where(correlationId: cor_ids).pluck(:stableId).uniq

# StatLog.where(meta_detail_id: meta_detail, ingested: false).count
# pr_stable_ids = Practitioner.where(meta_detail_id: meta_detail).pluck(:stableId).uniq
# PractitionerRole.where(meta_detail_id: meta_detail).count

total_count = pr_stable_ids.count
count = 1
provider_tenant_index_rk = Utils::Property.new.fetch_routing_key(meta_detail, 'provider_tenant_index')
pr_role_tenant_index_rk = Utils::Property.new.fetch_routing_key(meta_detail, 'practitioner_role_tenant_index')

pr_stable_ids.in_groups_of(25, false).each do |stable_ids|
  puts "#{count}:::#{total_count}"
  count += 1
  message = { meta_id: meta_detail.id, stable_ids: stable_ids, source_name: meta_detail.source.name }
  params = { routing_key: provider_tenant_index_rk, timestamp: Time.now.to_i }
  Notification.send_notification(message.as_json, params)

  params = { routing_key: pr_role_tenant_index_rk, timestamp: Time.now.to_i }
  Notification.send_notification(message.as_json, params)
end; 1

# -----------------------------------------------------

# =================================================================================================




















































#-------------------------------------------------------
# facility

batch_ids = ["facility:2c56a779-c28c-4c09-ab59-023b0fb9cf1f"]
source_name = 'wa-kaiserwa'
source = Source.find_by(name: source_name)
profile = 'http://hl7.org/fhir/us/core/STU5.0.1/StructureDefinition/us-core-OrganizationAffiliation'
meta_detail = MetaDetail.where(source:,profile:).order('created_at').last
retire_meta_details = MetaDetail.where(batchId: batch_ids).pluck(:id)
meta_detail

if meta_detail.present? && retire_meta_details.present?
  message = { meta_detail_id: meta_detail.id, retire_meta_detail_ids: retire_meta_details, send_notification: false }
  params = { routing_key: ENV['JOB_DELETE_BATCH_ROUTING_KEY'], timestamp: Time.now.to_i }
  Notification.send_notification(message.as_json, params)
end
# -----------------------------------------------
# Provider

batch_ids = ["provider:1bd39e6d-c4d5-46e3-b43c-267a83d87aca"]
source_name = 'wa-deltadental'
source = Source.find_by(name: source_name)
profile = 'http://hl7.org/fhir/us/core/StructureDefinition/us-core-practitionerrole'
meta_detail = MetaDetail.where(source:,profile:).order('created_at').last
retire_meta_details = MetaDetail.where(batchId: batch_ids).pluck(:id)
meta_detail

if meta_detail.present? && retire_meta_details.present?
  message = { meta_detail_id: meta_detail.id, retire_meta_detail_ids: retire_meta_details, send_notification: false }
  params = { routing_key: ENV['JOB_DELETE_BATCH_ROUTING_KEY'], timestamp: Time.now.to_i }
  Notification.send_notification(message.as_json, params)
end
