# RESULT HASH FOR SINGLE DOC INDEX

# ---------------------------------------------------------------------------------------
# Single DOC PROVIDER

def single_doc_provider_result_hash(tenant, input_source_name = nil)
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
  PractitionerRolesSingleDoc.index_name "practitioner_role_#{tenant}_rw"
  result_hash = { entity: 'Provider Single Doc', tenant:, index_name: "practitioner_role_#{tenant}_rw", ENV: Rails.env.to_s }.as_json
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
    updated_dates = meta_details.collect { |i| [i.batchId, i.index_time.strftime('%Y-%m-%dT%H:%M:%S.%LZ')] }
    result_hash[source_name] = {} unless result_hash[source_name].present?

    updated_dates.each do |batch_id, updated|
      res_count = PractitionerRolesSingleDoc.query({ "bool": { "must": [{ "match": { "meta.source": { "query": "#{source_name}", "operator": "and" } } }, { "match": { "meta.updated": "#{updated}" } }] } }).count
      res_meta = MetaDetail.where(batchId: batch_id).first
      plan_year_json = Practitioner.connection.execute("SELECT p.period
                            FROM practitioner_roles p
                            WHERE p.meta_detail_id = '#{res_meta.id}' LIMIT 1").to_a
      plan_year = JSON.parse(plan_year_json[0]['period'])['planYear'] rescue ''
      year = plan_year.present? ? "#{plan_year}::::" : ''
      result_hash[source_name]["#{year}#{batch_id}::::#{updated}"] = res_count if (res_count > 0 || print_all_batch_id)
    end
    # entity_db_count = Practitioner.where(source_id: source.id).count

    entity_db_count = if include_source_names.include?(source_name)
                        Practitioner.connection.execute("SELECT COUNT(DISTINCT p2.id)
                                           FROM practitioners p1
                                           JOIN practitioners p2 ON p1.\"stableId\" = p2.\"stableId\"
                                           WHERE p1.source_id IN (#{must_source_ids_str})
                                             AND p2.source_id = '#{source.id}'").to_a[0]['count']
                      else
                        Practitioner.connection.execute("SELECT COUNT(*)
                                                            FROM practitioners p
                                                            WHERE p.source_id = '#{source.id}'").to_a[0]['count']
                      end

    result_hash[source_name]['total_records_in_db'] = entity_db_count
    entity_es_count = PractitionerRolesSingleDoc.query({ "match": { "meta.source": { "query": "#{source_name}", "operator": "and" } } }).count
    result_hash[source_name]['total_records_in_es'] = entity_es_count
    result_hash[source_name]['ingestionMode'] = latest_meta_detail.processRules&.dig('ingestionMode')
    result_hash[source_name]['difference'] = entity_db_count - entity_es_count
    result_hash[source_name]['processed'] = latest_meta_detail.processed
    result_hash.delete(source_name) if rm_proper_source_from_result && result_hash[source_name]['difference'] == 0 && result_hash[source_name]['processed'] == true
  end; 1
  result_hash = result_hash.to_json
end


# puts result_hash = single_doc_provider_result_hash('honestmedical', 'hmg-honestpractitioner')
# puts JSON.generate(result_hash)
# result_array = []
# %w[wahbe centerlight riverspring honestmedical mddoh].each do |t|
#   puts "\n #{t}:"
#   result_array << single_doc_provider_result_hash(t)
# end;1
# puts result_array

# ---------------------------------------------------------------------------------------
# Single DOC FACILITY

def single_doc_facility_result_hash(tenant, input_source_name = nil)
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
  OrganizationAffiliationSingleDoc.index_name "organization_affiliation_#{tenant}_rw"
  result_hash = { entity: 'Facility Single Doc', tenant:, index_name: "organization_affiliation_#{tenant}_rw", ENV: Rails.env.to_s }.as_json
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
    updated_dates = meta_details.collect { |i| [i.batchId, i.index_time.strftime('%Y-%m-%dT%H:%M:%S.%LZ')] }
    result_hash[source_name] = {} unless result_hash[source_name].present?
    updated_dates.each do |batch_id, updated|
      res_count = OrganizationAffiliationSingleDoc.query({ "bool": { "must": [{ "match": { "meta.source": { "query": "#{source_name}", "operator": "and" } } }, { "match": { "meta.updated": "#{updated}" } }] } }).count
      res_meta = MetaDetail.where(batchId: batch_id).first
      plan_year_json = OrganizationAffiliation.connection.execute("SELECT p.period
                            FROM organization_affiliations p
                            WHERE p.meta_detail_id = '#{res_meta.id}' LIMIT 1").to_a
      plan_year = JSON.parse(plan_year_json[0]['period'])['planYear'] rescue ''
      year = plan_year.present? ? "#{plan_year}::::" : ''
      result_hash[source_name]["#{year}#{batch_id}::::#{updated}"] = res_count if (res_count > 0 || print_all_batch_id)
    end
    # entity_db_count = OrganizationAffiliation.where(source_id: source.id).count

    entity_db_count = if include_source_names.include?(source_name)
                        OrganizationAffiliation.connection.execute("SELECT COUNT(DISTINCT p2.id)
                                           FROM organization_affiliations p1
                                           JOIN organization_affiliations p2 ON p1.\"stableId\" = p2.\"stableId\"
                                           WHERE p1.source_id IN (#{must_source_ids_str})
                                             AND p2.source_id = '#{source.id}'").to_a[0]['count']
                      else
                        OrganizationAffiliation.connection.execute("SELECT COUNT(*)
                                                            FROM organization_affiliations p
                                                            WHERE p.source_id = '#{source.id}'").to_a[0]['count']
                      end

    result_hash[source_name]['total_records_in_db'] = entity_db_count
    entity_es_count = OrganizationAffiliationSingleDoc.query({ "match": { "meta.source": { "query": "#{source_name}", "operator": "and" } } }).count
    result_hash[source_name]['total_records_in_es'] = entity_es_count
    result_hash[source_name]['ingestionMode'] = latest_meta_detail.processRules&.dig('ingestionMode')
    result_hash[source_name]['difference'] = entity_db_count - entity_es_count
    result_hash[source_name]['processed'] = latest_meta_detail.processed
    result_hash.delete(source_name) if rm_proper_source_from_result && result_hash[source_name]['difference'] == 0 && result_hash[source_name]['processed'] == true
  end; 1
  result_hash
end

# result_hash = single_doc_facility_result_hash('wahbe', 'wa-cc-medicaid')
# puts JSON.generate(result_hash)

# result_array = []
# %w[wahbe centerlight riverspring honestmedical mddoh].each do |t|
#   puts "\n #{t}:"
#   result_array << single_doc_facility_result_hash(t)
# end;1
# puts JSON.generate(result_array)

# =================================================================================================
# -----------------------------------------------------
# FIX SINGLE DOC PROVIDER INDEX (Delete Excess records)

def rm_single_doc_provider_old_data_es(tenant, input_source_name = nil)
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

    index_name = "practitioner_role_#{tenant}_rw"
    PractitionerRolesSingleDoc.index_name(index_name)

    es_ids = PractitionerRolesSingleDoc.query({ "match": { "meta.source": { "query": "#{source.name}", "operator": "and" } } }).pluck(:id)
    # db_ids = PractitionerRole.where(source_id: source.id).pluck(:id)

    if include_source_names.include?(source_name)
      db_ids_res = PractitionerRole.connection.execute("SELECT pr.id
                                              FROM practitioner_roles pr
                                              WHERE pr.practitioner_id IN (
                                                SELECT DISTINCT p2.id
                                                FROM practitioners p1
                                                JOIN practitioners p2 ON p1.\"stableId\" = p2.\"stableId\"
                                                WHERE p1.source_id IN (#{must_source_ids_str})
                                                AND p2.source_id = '#{source.id}'
                                              )").to_a
      db_ids = db_ids_res.map { |i| i['id'] }
    else
      db_ids = PractitionerRole.where(source_id: source.id).pluck(:id)
    end

    next if db_ids.count == es_ids.count

    pr_role_ids = db_ids.count > es_ids.count ? db_ids - es_ids : es_ids - db_ids

    count = 0
    total_pr_count = pr_role_ids.count

    pr_role_ids_to_delete = []
    pr_role_ids_to_update = []

    pr_role_ids.each do |pr_id|
      # PractitionerRole.find_by(id: pr_id).present? ? pr_role_ids_to_update << pr_id : pr_role_ids_to_delete << pr_id
      pr_obj = PractitionerRole.find_by(id: pr_id)
      if pr_obj.present?
        stable_id = pr_obj.practitioner.stableId
        source_ids = Practitioner.where(stableId: stable_id).pluck(:source_id).uniq
        current_source_names = Source.where(id: source_ids).pluck(:name)
        tenant_source_names = Utils::Property.new.get_source_by_tenant(tenant, false)

        update = if tenant_source_names.present? && tenant_source_names.include?('all')
                   true
                 elsif tenant_source_names.present?
                   current_source_names.any? { |s| tenant_source_names.include?(s) }
                 else
                   true
                 end
        update == true ? pr_role_ids_to_update << pr_id : pr_role_ids_to_delete << pr_id
      else
        pr_role_ids_to_delete << pr_id
      end
    end

    pr_role_ids_to_delete.in_groups_of(1000, false).each do |root_ids_to_be_deleted|
      puts "Sending to Delete:::#{total_pr_count}"
      message = { meta_detail_id: meta_detail.id, batch_id: meta_detail.batchId, root_ids_to_be_deleted: root_ids_to_be_deleted, source_name: meta_detail.source.name }
      params = { routing_key: ENV['DELETE_MASTER_RECORD_ROUTING_KEY'], timestamp: Time.now.to_i }
      Notification.send_notification(message.as_json, params)

      params = { routing_key: ENV['DELETE_PR_ROLE_RECORD_ROUTING_KEY'], timestamp: Time.now.to_i }
      Notification.send_notification(message.as_json, params)
    end

    PrRoleDedup.where(practitioner_role_id: pr_role_ids_to_update).delete_all if pr_role_ids_to_update.present?
    row_hashes = PractitionerRole.where(id: pr_role_ids_to_update).pluck(:rowHash)
    pr_stable_ids = Practitioner.where(rowHash: row_hashes).pluck(:stableId).uniq

    pr_stable_ids.in_groups_of(25, false).each do |stable_ids|
      puts "Sending to update:::#{total_pr_count}"
      message = { meta_id: meta_detail.id, stable_ids: stable_ids, source_name: meta_detail.source.name }
      params = { routing_key: ENV['INDEX_PR_ROLE_TENANT_MV_ROUTING_KEY'], timestamp: Time.now.to_i }
      Notification.send_notification(message.as_json, params)
    end
    delete_report[source_name] = {} unless delete_report[source_name].present?
    delete_report[source_name]['total_diff'] = total_pr_count
    delete_report[source_name]['total_send_for_update'] = pr_role_ids_to_update.count
    delete_report[source_name]['total_send_for_delete'] = pr_role_ids_to_delete.count
  end
  delete_report
end

# delete_report = rm_single_doc_provider_old_data_es('honestmedical')
# delete_report = rm_single_doc_provider_old_data_es('wahbe', 'wa-cc-medicaid')
# puts JSON.generate(delete_report)

# -----------------------------------------------------
# FIX SINGLE DOC FACILITY INDEX (Delete Excess records)

def rm_single_doc_facility_old_data_es(tenant, input_source_name = nil)
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

    index_name = "organization_affiliation_#{tenant}_rw"
    OrganizationAffiliationSingleDoc.index_name(index_name)

    es_ids = OrganizationAffiliationSingleDoc.query({ "match": { "meta.source": { "query": "#{source.name}", "operator": "and" } } }).pluck(:id)
    # db_ids = OrganizationAffiliation.where(source_id: source.id).pluck(:id)

    if include_source_names.include?(source_name)
      db_ids_res = OrganizationAffiliation.connection.execute("SELECT DISTINCT p2.id
                                           FROM organization_affiliations p1
                                           JOIN organization_affiliations p2 ON p1.\"stableId\" = p2.\"stableId\"
                                           WHERE p1.source_id IN (#{must_source_ids_str})
                                             AND p2.source_id = '#{source.id}'").to_a

      db_ids = db_ids_res.map { |i| i['id'] }
    else
      db_ids = OrganizationAffiliation.where(source_id: source.id).pluck(:id)
    end

    next if db_ids.count == es_ids.count

    org_aff_ids = db_ids.count > es_ids.count ? db_ids - es_ids : es_ids - db_ids

    count = 0
    total_org_count = org_aff_ids.count

    org_aff_ids_to_delete = []
    org_aff_ids_to_update = []

    org_aff_ids.each do |org_id|
      # OrganizationAffiliation.find_by(id: org_id).present? ? org_aff_ids_to_update << org_id : org_aff_ids_to_delete << org_id
      org_obj = OrganizationAffiliation.find_by(id: org_id)
      if org_obj.present?
        stable_id = org_obj.stableId
        source_ids = OrganizationAffiliation.where(stableId: stable_id).pluck(:source_id).uniq
        current_source_names = Source.where(id: source_ids).pluck(:name)
        tenant_source_names = Utils::Property.new.get_source_by_tenant(tenant, false)

        update = if tenant_source_names.present? && tenant_source_names.include?('all')
                   true
                 elsif tenant_source_names.present?
                   current_source_names.any? { |s| tenant_source_names.include?(s) }
                 else
                   true
                 end
        update == true ? org_aff_ids_to_update << org_id : org_aff_ids_to_delete << org_id
      else
        org_aff_ids_to_delete << org_id
      end
    end


    org_aff_ids_to_delete.in_groups_of(1000, false).each do |root_ids_to_be_deleted|
      puts "Sending to Delete:::#{total_org_count}"
      message = { meta_detail_id: meta_detail.id, batch_id: meta_detail.batchId, root_ids_to_be_deleted: root_ids_to_be_deleted, source_name: meta_detail.source.name }
      params = { routing_key: ENV['DELETE_MASTER_RECORD_ROUTING_KEY'], timestamp: Time.now.to_i }
      Notification.send_notification(message.as_json, params)

      params = { routing_key: ENV['DELETE_ORG_AFF_RECORD_ROUTING_KEY'], timestamp: Time.now.to_i }
      Notification.send_notification(message.as_json, params)
    end

    OrgAffDedup.where(organization_affiliation_id: org_aff_ids_to_update).delete_all if org_aff_ids_to_update.present?
    org_stable_ids = OrganizationAffiliation.where(id: org_aff_ids_to_update).pluck(:stableId).uniq

    org_stable_ids.in_groups_of(25, false).each do |stable_ids|
      puts "Sending to update:::#{total_org_count}"
      message = { meta_id: meta_detail.id, stable_ids: stable_ids, source_name: meta_detail.source.name }
      params = { routing_key: ENV['INDEX_ORG_AFF_TENANT_MV_ROUTING_KEY'], timestamp: Time.now.to_i }
      Notification.send_notification(message.as_json, params)
    end
    delete_report[source_name] = {} unless delete_report[source_name].present?
    delete_report[source_name]['total_diff'] = total_org_count
    delete_report[source_name]['total_send_for_update'] = org_aff_ids_to_update.count
    delete_report[source_name]['total_send_for_delete'] = org_aff_ids_to_delete.count
  end
  delete_report
end

# delete_report = rm_single_doc_facility_old_data_es('mddoh')
# delete_report = rm_single_doc_facility_old_data_es('wahbe', 'or-uhc')
# puts JSON.generate(delete_report)

# --------------------------------------
