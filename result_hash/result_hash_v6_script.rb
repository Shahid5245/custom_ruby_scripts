
class ResultHash
  
  def self.result_hash_for_cluster_doc_org(tenant, input_source_name = nil)
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

  def self.result_hash_for_cluster_doc_pr(tenant, input_source_name = nil)
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

  def self.result_hash_for_single_doc_org(tenant, input_source_name = nil)
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

  def self.result_hash_for_single_doc_pr(tenant, input_source_name = nil)
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
    result_hash
  end

end