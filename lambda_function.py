import os
import logging
import traceback
import boto3
import jsonpickle
import json
import io
import time
from datetime import date, datetime as dt, timedelta, timezone
#import datetime as dt
import botocore
from botocore.errorfactory import ClientError
from collections import Counter
from collections import defaultdict
import tiktoken
from io import StringIO
import csv
import re
from collections import OrderedDict
from pandas import DataFrame
from typing import Any

tiktoken_cache_dir = "./tiktoken_cache"
os.environ["TIKTOKEN_CACHE_DIR"] = tiktoken_cache_dir
#os.environ["TIKTOKEN_CACHE_DIR"] = tiktoken_cache
tiktoken_cache_filename = "6494e42d5aad2bbb441ea9793af9e7db335c8d9c"
tiktoken_encoding = "o200k_base" #moved from cl100k_base to o200k_base for 4o mini 01 Nov 2024


maxBudgetTokensLLM = 999999  #NEW 01NOV24 128k is token limit of gpt-4omini but leaving 2k for 1.4k base prompt #7674 is token limit of gpt-4 but leaving 2k for 1.4k base prompt and some overhead for safety, was 7200 but got INTERNAL_ERROR Failed to generate Einstein LLM generations response until reduced by about half
maxBudgetTokensBasePromptLLM = 5000+10000 #5k for base prompt text and 10k for intermediary prompt transition text between counted elements

s3_client = boto3.client("s3")


def lambda_handler(event, context):
    extraTokensAvailLLM = 0
    ##str(logger.info('## ENVIRONMENT VARIABLES\r')) + jsonpickle.encode(dict(**os.environ))
    logger.info('## EVENT\r' + jsonpickle.encode(event))
    logger.info('## CONTEXT\r' + jsonpickle.encode(context))
    #response = client.get_account_settings()
    if event.get('wd_tenant_alias',None) is None:
        WD_TENANT_ALIAS = 'salesforce_wcpdev1'
        logger.info('## INFO:DEFAULTED_KEYVAR_EVENT ## Defaulting WD_TENANT_ALIAS to salesforce_wcpdev1')    
        BUCKET_NAME = 'workday-wcp-ffmmnb-new-us-west-2'
        OBJECT_NAME = 'einsteinGPTPromptBuilder/salesforce_wcpdev1/scheduledProcesses_futureReportsShared.json'
        OBJECT_NAME2 = 'einsteinGPTPromptBuilder/salesforce_wcpdev1/allCustomReports_runHistoryFieldsIncluded.json'
        OBJECT_NAME3 = 'einsteinGPTPromptBuilder/salesforce_wcpdev1/standardReports_allfields.json'
        OBJECT_NAME4 = 'einsteinGPTPromptBuilder/salesforce_wcpdev1/locationsUsedAsBusinessSite.json'
        OBJECT_NAME5 = 'einsteinGPTPromptBuilder/salesforce_wcpdev1/supervisoryOrganizations.json'
        OBJECT_NAME6 = 'einsteinGPTPromptBuilder/salesforce_wcpdev1/jobProfiles_MgmtCompGrades.json'
        WD_EXT_OPERATION = 'einsteinGPTPromptBuilder_reportLibraryMetadataPromptUserGenCard'
    else:
        WD_TENANT_ALIAS = str(event['wd_tenant_alias']) # or 'salesforce_wcpdev1'
        BUCKET_NAME = str(event.get('aws_s3_bucket','workday-wcp-ffmmnb-new-us-west-2')) # or 'workday-wcp-ffmmnb-us-west-2'
        OBJECT_NAME = str(event.get('aws_s3_filepath','einsteinGPTPromptBuilder/'+str(event['wd_tenant_alias'])+'/scheduledProcesses_futureReportsShared.json'))
        OBJECT_NAME2 = str(event.get('aws_s3_filepath2','einsteinGPTPromptBuilder/'+str(event['wd_tenant_alias'])+'/allCustomReports_runHistoryFieldsIncluded.json'))
        OBJECT_NAME3 = str(event.get('aws_s3_filepath3','einsteinGPTPromptBuilder/'+str(event['wd_tenant_alias'])+'/standardReports_allfields.json'))
        OBJECT_NAME4 = str(event.get('aws_s3_filepath4','einsteinGPTPromptBuilder/'+str(event['wd_tenant_alias'])+'/locationsUsedAsBusinessSite.json'))
        OBJECT_NAME5 = str(event.get('aws_s3_filepath5','einsteinGPTPromptBuilder/'+str(event['wd_tenant_alias'])+'/supervisoryOrganizations.json'))
        OBJECT_NAME6 = str(event.get('aws_s3_filepath6','einsteinGPTPromptBuilder/'+str(event['wd_tenant_alias'])+'/jobProfiles_MgmtCompGrades.json'))
        WD_EXT_OPERATION = str(event.get('wd_ext_operation','reportLibraryMetadataPromptUserGenCard')) # or 'scheduledProcesses-username'
        logger.info('## INFO:PROVIDED_KEYVAR_EVENT ## Received event WD_TENANT_ALIAS of '+str(event['wd_tenant_alias']))    

    #TODO JL16APR25 MUST Shreya before UAT remove this defaulting after all are specifying wd_tenant_alias every time
    if event.get('wd_tenant_alias') is None:
        wd_tenant_alias = 'salesforce_wcpdev1'
        logger.info('## INFO:DEFAULTED_KEYVAR_EVENT ## Defaulting WD_TENANT_ALIAS to salesforce_wcpdev1')
    else:
        wd_tenant_alias = str(event['wd_tenant_alias'])
    wd_username = str(event.get('wd_username','lmcneil'))

    #TODO-010 JL 17May24 revise to configurable base prompt in integration system attachment (and non-worker areas) or TC CF and eventually on to PF CRM prompt manager API long term? TECH DEBT MUST as attach integ system by end of 2025
    global sf_einstein_base_prompt
    global sf_all_data_sources_sec_check
    global sf_assignable_roles_sec_check
    sf_einstein_base_prompt = 'einsteinGPTPromptBuilder/'+str(WD_TENANT_ALIAS)+'/basePromptStatic.txt'
    #wd_secgroups = str(event.get('wd_sec_groups_roles',[{"wid":"c06bd843e6d201c01485ac8ed14f2500"},{"wid":"00707eaf108343a081a3f0a694743e67"}]))
    sf_all_data_sources_sec_check = 'einsteinGPTPromptBuilder/'+str(WD_TENANT_ALIAS)+'/allDataSources__SEC_CHECK.json'
    sf_assignable_roles_sec_check = 'einsteinGPTPromptBuilder/'+str(WD_TENANT_ALIAS)+'/assignableRoles__SEC_CHECK.json'
    event_mock_wait_time = event.get('mock_wait_time',None)
    event_libcard_prompt_length_max = event.get('libcard_prompt_length_max',maxBudgetTokensLLM)
    if event_libcard_prompt_length_max < maxBudgetTokensBasePromptLLM or event_libcard_prompt_length_max > maxBudgetTokensLLM:
        logger.info('## INFO:DEFAULTED_LIBCARD_PROMPT_LEN_EVENT ## Defaulting libcard_prompt_length_max to maxBudgetTokensLLM of '+str(maxBudgetTokensLLM)+' due to not specified value or out of bounds '+str(event_libcard_prompt_length_max))
        event_libcard_prompt_length_max = maxBudgetTokensLLM
    maxTokensAvailLLM_jobProfiles_MgmtCompGrades = (event_libcard_prompt_length_max-maxBudgetTokensBasePromptLLM)*0.25#0.35
    maxTokensAvailLLM_locationsUsedAsBusinessSite = (event_libcard_prompt_length_max-maxBudgetTokensBasePromptLLM)*0.011#0.15
    maxTokensAvailLLM_supervisoryOrganizations = (event_libcard_prompt_length_max-maxBudgetTokensBasePromptLLM)*0.689#0.10
    logger.info('## INFO:PROMPT_LENGTH_MAX ## LibCard Prompt Length Sup Org level max is '+str(maxTokensAvailLLM_supervisoryOrganizations))
    maxTokensAvailLLM_allCustomReportsAndStandardReports = (event_libcard_prompt_length_max-maxBudgetTokensBasePromptLLM)*0.05#0.4
    logger.info('## INFO:PROMPT_LENGTH_MAX ## LibCard Prompt Length Max is '+str(event_libcard_prompt_length_max)+' and max allocated total is '+str(maxTokensAvailLLM_jobProfiles_MgmtCompGrades+maxTokensAvailLLM_locationsUsedAsBusinessSite+maxTokensAvailLLM_supervisoryOrganizations+maxTokensAvailLLM_allCustomReportsAndStandardReports))
    wd_report_ids_username_match = {}
    wd_report_comments_match = {}
    wd_num_report_ids = int(0)
    wd_num_report_id_dict2 = int(0)
    wd_report_id_temp =str('')
    logger.info('## DIAG event_mock_wait_time is \r' + str(event_mock_wait_time))
    s3_client_head_object = boto3.client("s3")
    S3_BUCKET = BUCKET_NAME #'workday-wcp-ffmmnb-new-us-west-2'
    #TODO JL 16APR25 SHOULD by end of 2025 make customized prompt cards for common users refreshed only every 2-3 days for faster response and reduced compute usage, make tech debt item at GO LIVE of agent for backlog
    '''report_lib_user_card = 'einsteinGPTPromptBuilder/'+str(WD_TENANT_ALIAS)+'/user_cards/'+str(wd_username)+'-reportLibraryMetadataPromptUserGenCard.json'
    try:
        result_s3_head_object = s3_client_head_object.head_object(Bucket=S3_BUCKET, Key=report_lib_user_card)
        logger.info(('## S3 head_object HTTPStatusCode is ') +str(result_s3_head_object['ResponseMetadata']['HTTPStatusCode'])+' and x-amz-server-side-encryption is '+str(result_s3_head_object['ResponseMetadata']['HTTPHeaders']['x-amz-server-side-encryption'])+' and x-amz-request-id is '+str(result_s3_head_object['ResponseMetadata']['HTTPHeaders']['x-amz-request-id']))
        logger.info(('## Existing library card ') +str(report_lib_user_card))
        new_card_user = False
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            logger.info(('## EVT:NEW_CARD_USER ## ')+str(wd_username)+' >> '+str(S3_BUCKET)+' key '+str(report_lib_user_card))
            new_card_user = True
        elif e.response['Error']['Code'] == 403:
            logger.error(('## ERR no access to bucket or does not exist 403'))
        else:
            logger.error(('## ERR Other non 400 403 error valid creds?'))
            logging.error(traceback.format_exc())
    '''
    
    # json > csv

    bucket_name = str(event.get('_aws_s3_bucket', 'workday-wcp-ffmmnb-new-us-west-2'))
    logger.info(f"Using bucket: {bucket_name}")
    logger.info(f"Using tenant: {wd_tenant_alias}")

    # Extracting operation
    wd_ext_operation = event.get("wd_ext_operation", "").lower()
    uuid = event.get("uuid")
    files = event.get("files", [])
    logger.info("Using wd_ext_operation: %s", event.get("wd_ext_operation", "").lower())

    # Construct final S3 prefix based on convention
    # TODO JL 16APR25 move this into functions and only pass var from event to simplify COULD HAVE priority
    uuid_folder_prefix = f"einsteinGPTPromptBuilder/{wd_tenant_alias}/tmp_final_data_pull/{uuid}/"
    logger.info(f"Operation: {wd_ext_operation}")
    logger.info(f"S3 prefix for processing: {uuid_folder_prefix}")

    sec_roles = event.get("wd_sec_groups_roles", {})
    wd_data_source = event.get("wd_data_source", "")
    def extract_user_and_role_wids(sec_groups_roles):
        user_wids = {entry["wid"] for entry in sec_groups_roles.get("user_BasedSecurityGroupsForUser", []) if "wid" in entry}
        role_wids = {entry["wid"] for entry in sec_groups_roles.get("organizationRoleAssignments__roleName", []) if "wid" in entry}

        # Add known elevated WID temporarily
        #user_wids.add("00707eaf108343a081a3f0a694743e67") # e.g., HR Partner
        return user_wids, role_wids
    #logger.info(f"sec_roles",{sec_roles})
    #logger.info(f"wd_data_source",{wd_data_source})

    # Route operation
    global file_content
    if wd_ext_operation == "agentforcerecentuserqueries":
        if not wd_username or not wd_tenant_alias: 
            logger.error("Missing required fields: wd_tenant_alias or wd_username.")
            file_content = {"lambda_errors": [
            {
            "reportLibrarylambda-amplify-einsteinRepLibMetaCard-DEV-ERR-001: Missing required fields: wd_tenant_alias or wd_username. "+str(wd_ext_operation)+" in aws_request_id "+ jsonpickle.encode(str(context.aws_request_id)) +" at "+ str(dt.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f%Z"))
            }]}
            return file_content
        else:
            return_recent_user_queries(bucket_name, wd_tenant_alias, wd_username, context)
    elif wd_ext_operation == "agentforcejson2csv":
        if not uuid or not files: 
            logger.error("Missing required fields: wd_ext_operation, uuid, or files.")
            file_content = {"lambda_errors": [
            {
            "reportLibrarylambda-amplify-einsteinRepLibMetaCard-DEV-ERR-002: Missing required fields: wd_ext_operation, uuid, or files."+str(wd_ext_operation)+" in aws_request_id "+ jsonpickle.encode(str(context.aws_request_id)) +" at "+ str(dt.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f%Z"))
            }]}
            return file_content
            
        else:
            #TODO JL 16APR25 MUST before UAT move this down to json2csv and tmpdel areas diagnostic not relevant for others
            logger.info("Using uuid: %s", event.get("uuid"))
            logger.info("Using files: %s", event.get("files", []))

            # Expecting only one file
            if event.get('file_info',None) is None:
                logger.info("No files array present")
            else:
                file_info = files[0]
                file_name = file_info.get("file_name")
                file_size = file_info.get("size")
            process_all_json_to_csv_and_upload(bucket_name, uuid_folder_prefix, event, context)
    elif wd_ext_operation == "agentforcetmpdel":
        delete_combined_output_csv_file(bucket_name, uuid_folder_prefix, context)
    if wd_ext_operation == "agentforcePreviewDataPull":
        return handle_agentforce_preview_data_pull(event, context)
    elif wd_ext_operation == "reportlibrarymetadatapromptusergencard":
        if event_mock_wait_time is None:# and new_card_user is True:
            #Time to generate a new library card
            logger.info(('## No mock wait time was ')+str(event_mock_wait_time))#+' and new card user condition was '+str(new_card_user)+' for '+str(wd_username))
            #FILE_NAME = 'temporaryjl001.json'
            s3_client = boto3.client("s3")
            S3_BUCKET = BUCKET_NAME #'workday-wcp-ffmmnb-new-us-west-2'

            object_key = OBJECT_NAME4 #"einsteinGPTPromptBuilder/locationsUsedAsBusinessSite.json"  # replace object key
            logger.info('## INFO:S3_OPERATION_START ## Starting to retrieve S3 object '+object_key)    

            #global file_content
            try:
                file_content = s3_client.get_object(
                    Bucket=S3_BUCKET, Key=object_key)["Body"].read()
            except s3_client.exceptions.NoSuchKey:
                logger.warning("Unknown error when trying to retrieve WD metadata NoSuchKey (potential INT0294 staging long term or S3 lifecycle missing short term failure) on file: %s", object_key)
                file_content = {"lambda_errors": [
                  {
                  "reportLibrarylambda-amplify-einsteinRepLibMetaCard-DEV-ERR-003M01: Unexpected error when trying to retrieve WD metadata not NoSuchKey for "+object_key+" in aws_request_id "+ jsonpickle.encode(str(context.aws_request_id)) +" at "+ str(dt.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f%Z"))
                  }
                ]}
                return file_content
            except s3_client.exceptions.ClientError as e:            
                logging.error(traceback.format_exc())
                logger.warning("Unknown error when trying to retrieve WD metadata not NoSuchKey: %s", object_key)
                file_content = {"lambda_errors": [
                  {
                  "reportLibrarylambda-amplify-einsteinRepLibMetaCard-DEV-ERR-003M02: Unexpected error when trying to retrieve WD metadata not NoSuchKey for "+object_key+" in aws_request_id "+ jsonpickle.encode(str(context.aws_request_id)) +" at "+ str(dt.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f%Z"))
                  }
                ]}
                return file_content

            logger.info(('## Lambda locationsUsedAsBusinessSite file length was '+str(len(file_content))))#+' and token use would be '+str(num_tokens_from_string(str(file_content), tiktoken_encoding))))
            #token_fulluse_total += num_tokens_from_string(str(file_content), tiktoken_encoding)
            #print(file_content)
            read_data = file_content

            try:
                locData = json.loads(read_data)
            except json.JSONDecodeError as e:
                logger.warning("Unknown error when trying to decode JSON of WD metadata: %s %s", e, object_key)
                file_content = {"lambda_errors": [
                  {
                  "reportLibrarylambda-amplify-einsteinRepLibMetaCard-DEV-ERR-003M02A: Unexpected  error when trying to decode JSON of WD metadata for "+object_key+" in aws_request_id "+ jsonpickle.encode(str(context.aws_request_id)) +" at "+ str(dt.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f%Z"))
                  }
                ]}
                return file_content
            except Exception as e:         
                logging.error(traceback.format_exc())
                logger.warning("Unknown error when trying to interpret WD metadata not JSONDecode: %s", object_key)
                file_content = {"lambda_errors": [
                  {
                  "reportLibrarylambda-amplify-einsteinRepLibMetaCard-DEV-ERR-003M02B: Unexpected error when trying to interpret WD metadata not JSONDecode for "+object_key+" in aws_request_id "+ jsonpickle.encode(str(context.aws_request_id)) +" at "+ str(dt.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f%Z"))
                  }
                ]}
                return file_content

            #read_data = data_stream.read() ###was f.read()

            #####data = json.loads(read_data)
            locData_output_json = {
                "locations": [],
                "total": locData["total"]
            }
            sorted_data_locationWorkerCount = sorted(locData["data"], key=lambda x: x.get("locationWorkerCount", 0), reverse=True)
            for data in sorted_data_locationWorkerCount:
                location_type = data.get("locationType", [])  # Default to empty list if missing
                locData_output_json["locations"].append({
                    #"locationTimeZone": data["locationTimeZone"]["descriptor"],
                    "location": data["location"]["descriptor"],
                    #"locationType": data["locationType"][0]["descriptor"],
                    #"locationType": location_type[0]["descriptor"] if location_type else "",  # Handle empty case safely
                    #"locationWorkerCount": data["locationWorkerCount"],
                    #"primaryAddress_FullWithCountry": data["primaryAddress_FullWithCountry"],
                    "countryRegion": data["countryRegion"]["descriptor"] if "countryRegion" in data else None,
                    "country": data["country"]["descriptor"]
                })
            promptTokensConsumed_locationsUsedAsBusinessSite = num_tokens_from_string(str(locData_output_json), tiktoken_encoding, context)
            logger.info("## INFO:TOKEN_COUNT ## Tokens consumed by locationsUsedAsBusinessSite before truncation is "+str(promptTokensConsumed_locationsUsedAsBusinessSite)+" versus of max tokens "+str(maxTokensAvailLLM_locationsUsedAsBusinessSite)+" out of entire prompt limit of "+str(event_libcard_prompt_length_max))
            if promptTokensConsumed_locationsUsedAsBusinessSite > maxTokensAvailLLM_locationsUsedAsBusinessSite: 
                charReduce=int((((maxTokensAvailLLM_locationsUsedAsBusinessSite/promptTokensConsumed_locationsUsedAsBusinessSite))*len(str(locData_output_json))))
                logger.info("## EVT:TRUNC_ML_METADATA ## locationsUsedAsBusinessSite prompt is "+str(promptTokensConsumed_locationsUsedAsBusinessSite)+" and needs reduction of full output length "+str(len(str(locData_output_json)))+" of "+str(charReduce)+" to meet max of "+str(maxTokensAvailLLM_locationsUsedAsBusinessSite))
                locData_output_json = (str(locData_output_json)[:charReduce]+"...\n\n###\nRemaining "+str(charReduce)+" characters of locationsUsedAsBusinessSite was truncated to fit LLM Gateway API token limits")
                #return output_data
            else:
                logger.info("## INFO:TOKEN_NOT_EXCEED ## No truncation required of locationsUsedAsBusinessSite due to "+str(type(locData_output_json))+"of token length "+str(promptTokensConsumed_locationsUsedAsBusinessSite)+" within max of "+str(maxTokensAvailLLM_locationsUsedAsBusinessSite))
                extraTokensAvailLLM += maxTokensAvailLLM_locationsUsedAsBusinessSite-promptTokensConsumed_locationsUsedAsBusinessSite
                locData_output_json = str(locData_output_json)

            s3_client = boto3.client("s3")
            S3_BUCKET = BUCKET_NAME #'workday-wcp-ffmmnb-new-us-west-2'

            object_key = OBJECT_NAME5 #"einsteinGPTPromptBuilder/supervisoryOrganizations.json"  # replace object key
            logger.info('## INFO:S3_OPERATION_START ## Starting to retrieve S3 object '+object_key)    
            try:
                file_content = s3_client.get_object(
                    Bucket=S3_BUCKET, Key=object_key)["Body"].read()
            except s3_client.exceptions.NoSuchKey:
                logger.warning("Unknown error when trying to retrieve WD metadata NoSuchKey (potential INT0294 staging long term or S3 lifecycle missing short term failure) on file: %s", object_key)
                file_content = {"lambda_errors": [
                  {
                  "reportLibrarylambda-amplify-einsteinRepLibMetaCard-DEV-ERR-003M03: Unexpected error when trying to retrieve WD metadata not NoSuchKey for "+object_key+" in aws_request_id "+ jsonpickle.encode(str(context.aws_request_id)) +" at "+ str(dt.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f%Z"))
                  }
                ]}
                return file_content
            except s3_client.exceptions.ClientError as e:            
                logging.error(traceback.format_exc())
                logger.warning("Unknown error when trying to retrieve WD metadata not NoSuchKey: %s", object_key)
                file_content = {"lambda_errors": [
                  {
                  "reportLibrarylambda-amplify-einsteinRepLibMetaCard-DEV-ERR-003M04: Unexpected error when trying to retrieve WD metadata not NoSuchKey for "+object_key+" in aws_request_id "+ jsonpickle.encode(str(context.aws_request_id)) +" at "+ str(dt.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f%Z"))
                  }
                ]}
                return file_content

            logger.info(('## Lambda supervisoryOrganizations file length was ') +str(len(file_content)))#+' and token use would be '+str(num_tokens_from_string(str(file_content), tiktoken_encoding)))
            #token_fulluse_total += num_tokens_from_string(str(file_content), tiktoken_encoding)
            #print(file_content)
            read_data = file_content            
            try:
                supOrgData = json.loads(read_data)
            except json.JSONDecodeError as e:
                logger.warning("Unknown error when trying to decode JSON of WD metadata: %s %s", e, object_key)
                file_content = {"lambda_errors": [
                  {
                  "reportLibrarylambda-amplify-einsteinRepLibMetaCard-DEV-ERR-003M04A: Unexpected  error when trying to decode JSON of WD metadata for "+object_key+" in aws_request_id "+ jsonpickle.encode(str(context.aws_request_id)) +" at "+ str(dt.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f%Z"))
                  }
                ]}
                return file_content
            except Exception as e:         
                logging.error(traceback.format_exc())
                logger.warning("Unknown error when trying to interpret WD metadata not JSONDecode: %s", object_key)
                file_content = {"lambda_errors": [
                  {
                  "reportLibrarylambda-amplify-einsteinRepLibMetaCard-DEV-ERR-003M04B: Unexpected error when trying to interpret WD metadata not JSONDecode for "+object_key+" in aws_request_id "+ jsonpickle.encode(str(context.aws_request_id)) +" at "+ str(dt.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f%Z"))
                  }
                ]}
                return file_content

            
            supOrgData_output_json = {"supervisoryOrganizations": [], "total": supOrgData["total"]}

            def parse_management_level_descriptor(descriptor: str):
                if not descriptor or len(descriptor.split()) == 0:
                    return float('inf')  # Sort unknown/missing levels to the end
                try:
                    return int(descriptor.split()[0])
                except ValueError:
                    return float('inf')  # If it's not a number like "Executive", "N/A", etc.

            for org in supOrgData["data"]:
                new_org = {}

                supervisor = org.get("supervisoryManager")
                if not supervisor:
                    logger.info('## INFO:supervisoryOrganization has no supervisoryManager')
                    continue

                management_level = supervisor.get("managementLevel", {}).get("descriptor", "") if supervisor else ""
                org_level = org.get("organizationLevelFromTop", None)
                employee_count_str = org.get("employeeCount_IncludeSubordinateOrganizationsIndexed", "0")
                
                try:
                    employee_count = int(employee_count_str.replace(',', ''))  # Convert to int safely
                except ValueError:
                    employee_count = 0

              
                include = False

                is_exec = management_level.startswith("1 Executive")
                is_l3 = org_level == 3
                is_l4 = org_level == 4
                is_other = org_level not in [3, 4]
                emp_count = employee_count  

                
                if is_exec:
                    include = True
                elif is_l3:
                    include = True
                elif is_l4 and emp_count > 10:#250:
                    include = True
                elif is_other and emp_count > 10:#250:
                    include = True
                elif emp_count > 10:#150:
                    include = True

                if not include:
                    continue  # Skip this org
                # Build a trimmed supervisoryManager
                trimmed_manager = {
                    "jobFamily": supervisor.get("jobFamily", [{}])[0].get("descriptor", ""),
                    "jobFamilyGroup": supervisor.get("jobFamilyGroup", [{}])[0].get("descriptor", ""),
                    "worker": supervisor.get("worker", {}).get("descriptor", ""),
                    "businessTitle": supervisor.get("businessTitle", ""),
                    "jobProfile": supervisor.get("jobProfile", {}).get("descriptor", ""),
                    "managementLevel": supervisor.get("managementLevel", {}).get("descriptor", "")
                }

                new_org["supervisoryManager"] = trimmed_manager
                new_org["organizationID"] = org.get("organization", {}).get("id", "")
                #new_org["employeeCount"] = employee_count  # <--- Include in output for debugging
                supOrgData_output_json["supervisoryOrganizations"].append(new_org)

            # Sort by managementLevel descriptor ASC, then employeeCount DESC
            supOrgData_output_json["supervisoryOrganizations"].sort(
                key=lambda x: (
                    parse_management_level_descriptor(
                        x.get("supervisoryManager", {}).get("managementLevel", "")
                    ),
                    -x.get("employeeCount", 0)  # employeeCount was not part of final output, so it's optional
                )
            )

            promptTokensConsumed_supervisoryOrganizations = num_tokens_from_string(str(supOrgData_output_json), tiktoken_encoding, context)
            logger.info("## INFO:TOKEN_COUNT ## Tokens consumed by supervisoryOrganizations before truncation is "+str(promptTokensConsumed_supervisoryOrganizations)+" versus of max tokens "+str(maxTokensAvailLLM_supervisoryOrganizations)+" out of entire prompt limit of "+str(event_libcard_prompt_length_max))
            if promptTokensConsumed_supervisoryOrganizations > maxTokensAvailLLM_supervisoryOrganizations: 
                charReduce=int((((maxTokensAvailLLM_supervisoryOrganizations/promptTokensConsumed_supervisoryOrganizations))*len(str(supOrgData_output_json))))
                logger.info("## EVT:TRUNC_ML_METADATA ## supervisoryOrganizations prompt is "+str(promptTokensConsumed_supervisoryOrganizations)+" and needs reduction of full output length "+str(len(str(supOrgData_output_json)))+" of "+str(charReduce)+" to meet max of "+str(maxTokensAvailLLM_supervisoryOrganizations))
                supOrgData_output_json = (str(supOrgData_output_json)[:charReduce]+"...\n\n###\nRemaining "+str(charReduce)+" characters of supervisoryOrganizations was truncated to fit LLM Gateway API token limits")
                #return output_data
            else:
                logger.info("## INFO:TOKEN_NOT_EXCEED ## No truncation required of supervisoryOrganizations due to "+str(type(supOrgData_output_json))+"of token length "+str(promptTokensConsumed_supervisoryOrganizations)+" within max of "+str(maxTokensAvailLLM_supervisoryOrganizations))
                extraTokensAvailLLM += maxTokensAvailLLM_supervisoryOrganizations-promptTokensConsumed_supervisoryOrganizations
                supOrgData_output_json = str(supOrgData_output_json)


            s3_client = boto3.client("s3")
            S3_BUCKET = BUCKET_NAME #'workday-wcp-ffmmnb-new-us-west-2'

            def load_json_from_s3(bucket, key, context):
                logger.info(f"## INFO:S3_OPERATION_START ## Starting to retrieve S3 object {key}")
                try:
                    response = s3_client.get_object(Bucket=bucket, Key=key)
                    content = response['Body'].read().decode('utf-8')
                    return json.loads(content)

                except s3_client.exceptions.NoSuchKey:
                    logger.warning(f"Missing S3 key (NoSuchKey): {key}")
                    return {
                        "lambda_errors": [
                            {
                                f"reportLibrarylambda-amplify-einsteinRepLibMetaCard-DEV-ERR-003M14: File not found in S3 bucket {bucket} for key {key}. aws_request_id {jsonpickle.encode(str(context.aws_request_id))} at {dt.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f%Z')}"
                            }
                        ]
                    }

                except s3_client.exceptions.ClientError as e:
                    logger.error(traceback.format_exc())
                    return {
                        "lambda_errors": [
                            {
                                f"reportLibrarylambda-amplify-einsteinRepLibMetaCard-DEV-ERR-003M15: ClientError when accessing S3 key {key} in bucket {bucket}. aws_request_id {jsonpickle.encode(str(context.aws_request_id))} at {dt.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f%Z')} Error: {str(e)}"
                            }
                        ]
                    }

                except Exception as e:
                    logger.error(traceback.format_exc())
                    return {
                        "lambda_errors": [
                            {
                                f"reportLibrarylambda-amplify-einsteinRepLibMetaCard-DEV-ERR-003M16: Unexpected error loading key {key} from bucket {bucket}. aws_request_id {jsonpickle.encode(str(context.aws_request_id))} at {dt.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f%Z')} Error: {str(e)}"
                            }
                        ]
                    }
            # Text file loader (base prompt)
            def load_text_lines_from_s3(bucket, key):
                logger.info('## INFO:S3_OPERATION_START ## Starting to retrieve S3 object ' + key)
                try:
                    file_content = s3_client.get_object(Bucket=bucket, Key=key)
                    content = file_content['Body'].read().decode('utf-8')
                    return [line.strip() for line in content.splitlines() if line.strip()]
                except s3_client.exceptions.NoSuchKey:
                    logger.warning("Missing basePromptStatic.txt file (NoSuchKey): %s", key)
                    return {
                        "lambda_errors": [
                            {
                                f"reportLibrarylambda-amplify-einsteinRepLibMetaCard-DEV-ERR-003M13: basePromptStatic.txt missing at {key} in aws_request_id {jsonpickle.encode(str(context.aws_request_id))} at {dt.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f%Z')}"
                            }
                        ]
                    }
                except s3_client.exceptions.ClientError as e:
                    logging.error(traceback.format_exc())
                    logger.warning("ClientError retrieving basePromptStatic.txt: %s", key)
                    return {
                        "lambda_errors": [
                            {
                                f"reportLibrarylambda-amplify-einsteinRepLibMetaCard-DEV-ERR-003M14: ClientError retrieving basePromptStatic.txt at {key} in aws_request_id {jsonpickle.encode(str(context.aws_request_id))} at {dt.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f%Z')}"
                            }
                        ]
                    }
                except Exception as e:
                    logging.error(traceback.format_exc())
                    logger.warning("Unexpected error reading basePromptStatic.txt: %s", key)
                    return {
                        "lambda_errors": [
                            {
                                f"reportLibrarylambda-amplify-einsteinRepLibMetaCard-DEV-ERR-003M15: Unexpected error reading basePromptStatic.txt at {key} with error {str(e)} in aws_request_id {jsonpickle.encode(str(context.aws_request_id))} at {dt.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f%Z')}"
                            }
                        ]
                    }

            # Now load the two JSON files from S3
            all_data_sources = load_json_from_s3(bucket_name, sf_all_data_sources_sec_check, context)
            if "lambda_errors" in all_data_sources:
                return all_data_sources

            assignable_roles = load_json_from_s3(bucket_name, sf_assignable_roles_sec_check, context)
            if "lambda_errors" in assignable_roles:
                return assignable_roles
            prompt_lines = load_text_lines_from_s3(bucket_name, sf_einstein_base_prompt)

            #object_key = OBJECT_NAME #"einsteinGPTPromptBuilder/scheduledProcesses_futureReportsShared.json"  # replace object key
            object_key = OBJECT_NAME2 #"einsteinGPTPromptBuilder/allCustomReports_runHistoryFieldsIncluded.json"  # replace object key
            logger.info('## INFO:S3_OPERATION_START ## Starting to retrieve S3 object '+object_key)    
            try:
                file_content = s3_client.get_object(
                    Bucket=S3_BUCKET, Key=object_key)["Body"].read()
            except s3_client.exceptions.NoSuchKey:
                logger.warning("Unknown error when trying to retrieve WD metadata NoSuchKey (potential INT0294 staging long term or S3 lifecycle missing short term failure) on file: %s", object_key)
                file_content = {"lambda_errors": [
                  {
                  "reportLibrarylambda-amplify-einsteinRepLibMetaCard-DEV-ERR-003M05: Unexpected error when trying to retrieve WD metadata not NoSuchKey for "+object_key+" in aws_request_id "+ jsonpickle.encode(str(context.aws_request_id)) +" at "+ str(dt.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f%Z"))
                  }
                ]}
                return file_content
            except s3_client.exceptions.ClientError as e:            
                logging.error(traceback.format_exc())
                logger.warning("Unknown error when trying to retrieve WD metadata not NoSuchKey: %s", object_key)
                file_content = {"lambda_errors": [
                  {
                  "reportLibrarylambda-amplify-einsteinRepLibMetaCard-DEV-ERR-003M06: Unexpected error when trying to retrieve WD metadata not NoSuchKey for "+object_key+" in aws_request_id "+ jsonpickle.encode(str(context.aws_request_id)) +" at "+ str(dt.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f%Z"))
                  }
                ]}
                return file_content

            logger.info(('## Lambda allCustomReports file length was ') +str(len(file_content)))#+' and token use would be '+str(num_tokens_from_string(str(file_content), tiktoken_encoding)))
            #token_fulluse_total = num_tokens_from_string(str(file_content), tiktoken_encoding)
            #print(file_content)
            read_data = file_content
            #read_data = data_stream.read() ###was f.read()

            #####data = json.loads(read_data)
            data = json.loads(read_data)
            wql_aliases = [item.get("dataSource__WQLAlias", {}).get("WQLAlias", "Unknown") for item in data["data"]]
            wql_alias_count = Counter(wql_aliases)
            sorted_wql_alias_count = sorted(wql_alias_count.items(), key=lambda x: x[1], reverse=True)


            object_key2 = OBJECT_NAME3 #"einsteinGPTPromptBuilder/standardReports_allfields.json"  # replace object key
            logger.info('## INFO:S3_OPERATION_START ## Starting to retrieve S3 object '+object_key2)    
            try:
                file_content = s3_client.get_object(
                    Bucket=S3_BUCKET, Key=object_key2)["Body"].read()
            except s3_client.exceptions.NoSuchKey:
                logger.warning("Unknown error when trying to retrieve WD metadata NoSuchKey (potential INT0294 staging long term or S3 lifecycle missing short term failure) on file: %s", object_key)
                file_content = {"lambda_errors": [
                  {
                  "reportLibrarylambda-amplify-einsteinRepLibMetaCard-DEV-ERR-003M07: Unexpected error when trying to retrieve WD metadata not NoSuchKey for "+object_key+" in aws_request_id "+ jsonpickle.encode(str(context.aws_request_id)) +" at "+ str(dt.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f%Z"))
                  }
                ]}
                return file_content
            except s3_client.exceptions.ClientError as e:            
                logging.error(traceback.format_exc())
                logger.warning("Unknown error when trying to retrieve WD metadata not NoSuchKey: %s", object_key)
                file_content = {"lambda_errors": [
                  {
                  "reportLibrarylambda-amplify-einsteinRepLibMetaCard-DEV-ERR-003M08: Unexpected error when trying to retrieve WD metadata not NoSuchKey for "+object_key+" in aws_request_id "+ jsonpickle.encode(str(context.aws_request_id)) +" at "+ str(dt.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f%Z"))
                  }
                ]}
                return file_content

            #print(file_content)
            logger.info(('## Lambda standardReports file length was ') +str(len(file_content)))#+' and token use would be '+str(num_tokens_from_string(str(file_content), tiktoken_encoding)))
            #token_fulluse_total += num_tokens_from_string(str(file_content), tiktoken_encoding)
            #logger.info(('## Lambda ALL JSON token use would be '+str(token_fulluse_total)))
            read_data = file_content

            input_json = json.loads(read_data)
            #logger.info(f"inputJson {input_json}")

            # Initialize a dictionary to store the summarized data
            summary = defaultdict(lambda: {"fieldsDisplayedOnReport": defaultdict(int), "numberOfTimesExecuted": 0})
            #logger.info(f"Checking if 'onLeave' appears in fields: {[field['WQLAlias'] for item in input_json['data'] for field in item.get('fieldsDisplayedOnReport', []) if 'WQLAlias' in field]}")

            
            # Iterate over the data in the input JSON
            merged_data = data["data"] + input_json["data"]
            
            # Extract user & role WIDs from event
            wd_sec_groups_roles = event.get("wd_sec_groups_roles", {})
            if isinstance(wd_sec_groups_roles, list):
                wd_sec_groups_roles = {}
            user_wids, role_wids = extract_user_and_role_wids(wd_sec_groups_roles)

            #user_wids, role_wids = extract_user_and_role_wids(event.get("wd_sec_groups_roles", {}))

            # Find allowed aliases
            allowed_aliases = get_allowed_wql_aliases(user_wids, role_wids, all_data_sources, assignable_roles)
            logger.info(f"Allowed Aliases {allowed_aliases}")
            global filtered_prompt
            filtered_prompt = generate_filtered_prompt(prompt_lines, allowed_aliases)
            #logger.info(f"Filtered prompt:\n{filtered_prompt}")
            
            # Iterate over the data in the input JSON
            for item in merged_data:
                # Check if the item has a dataSource__WQLAlias
                #print("primaryBusinessObject is equal to "+str(item.get("dataSource__WQLAlias", {}).get("primaryBusinessObject")))
                if "dataSource__WQLAlias" in item and str(item.get("dataSource__WQLAlias", {}).get("primaryBusinessObject")) == 'Worker':
                    # Get the WQLAlias
                    alias = item["dataSource__WQLAlias"]["WQLAlias"]
                    
                    # Only process if alias is in the allowed_aliases
                    if alias not in allowed_aliases:
                        logger.debug(f"Skipping alias '{alias}' as it's not in allowed_aliases.")
                        continue  # Skip this item
                    # Update the number of times executed                    
                    summary[alias]["numberOfTimesExecuted"] += item["numberOfTimesExecuted"]
                    #logger.info(f"TestnumberOfTimesExecuted {summary[alias]["numberOfTimesExecuted"]}")
                    if "fieldsDisplayedOnReport" in item:
                        # Iterate over the fields displayed on the report
                        for field in item["fieldsDisplayedOnReport"]:
                            if "WQLAlias" in field:
                                # Update the count for the field's WQLAlias
                                # Sum the number of report runs instead of just counting occurrences
                                summary[alias]["fieldsDisplayedOnReport"][field["WQLAlias"]] += item["numberOfTimesExecuted"]
                                #summary[alias]["fieldsDisplayedOnReport"][field["WQLAlias"]] += 1
                                #logger.info(f"TestsummaryaliasfieldsDisplayedOnReport {summary[alias]["fieldsDisplayedOnReport"][field["WQLAlias"]] }")
        
            # Convert the summarized data to the desired output format
            output_data = []
            '''
            for alias, data in sorted(summary.items(), key=lambda x: x[1]["numberOfTimesExecuted"], reverse=True):
                output_data.append({
                    "dataSource__WQLAlias": alias,
                    "fieldsDisplayedOnReport": [{"WQLAlias": k, "count": v} for k, v in sorted(data["fieldsDisplayedOnReport"].items(), key=lambda x: x[1], reverse=True)],
                    "numberOfTimesExecuted": data["numberOfTimesExecuted"]
                })
            logger.info(f"OutputData {output_data}")
            '''
            
            for alias, data in sorted(summary.items(), key=lambda x: x[1]["numberOfTimesExecuted"], reverse=True):  # Sorting by execution count DESC
                # Sort fields by sum of `numberOfTimesExecuted` in descending order
                sorted_fields = sorted(
                    (field for field in data["fieldsDisplayedOnReport"].items() if field[1] != 0),
                    key=lambda x: x[1], 
                    reverse=True
                )

                # Limit to 150 fields per data source
                top_fields = sorted_fields[:150]
                omitted_fields = sorted_fields[150:]
                omitted_count = len(omitted_fields)
                omitted_execution_sum = sum(v for _, v in omitted_fields)
                
                if omitted_count > 0:
                    logger.info(f"Omitted {omitted_count} fields from alias {alias}, total omitted executions: {omitted_execution_sum}")
                '''
                output_data.append({
                    "dataSource__WQLAlias": alias,
                    "fieldsDisplayedOnReport": [{"WQLAlias": k, "numberOfTimesExecuted": v} for k, v in top_fields],
                    "numberOfTimesExecuted": data["numberOfTimesExecuted"]
                })
                '''
                output_data.append({
                    "dataSource__WQLAlias": alias,
                    "fieldsDisplayedOnReport": {
                        "columns": ["WQLAlias", "numberOfTimesExecuted"],
                        "values": [[k, v] for k, v in top_fields]
                    },
                    "numberOfTimesExecuted": data["numberOfTimesExecuted"]
                })
            #logger.info(f"All custom: {output_data}")

            #logger.info(f"Processed Output Data: {output_data}")
            
            #total_on_leave = sum(data["fieldsDisplayedOnReport"].get("onLeave", 0) for _, data in summary.items())
            #logger.info(f"📢 Final count of 'onLeave' across all aliases: {total_on_leave}")
            #logger.info(f"Final count of 'onLeave' across reports: {sum(data['fieldsDisplayedOnReport'].get('onLeave', 0) for _, data in summary.items())}")

            #return json.dumps(output_data, indent=0)
            #return output_data
            # Print the output string
            promptTokensConsumed_allCustomReportsAndStandardReports = num_tokens_from_string(str(output_data), tiktoken_encoding, context)
            logger.info("## INFO:TOKEN_COUNT ## Tokens consumed by allCustomReportsAndStandardReports before truncation is "+str(promptTokensConsumed_allCustomReportsAndStandardReports)+" versus of max tokens "+str(maxTokensAvailLLM_allCustomReportsAndStandardReports)+" out of entire prompt limit of "+str(event_libcard_prompt_length_max))
            if promptTokensConsumed_allCustomReportsAndStandardReports > maxTokensAvailLLM_allCustomReportsAndStandardReports: 
                charReduce=int((((maxTokensAvailLLM_allCustomReportsAndStandardReports/promptTokensConsumed_allCustomReportsAndStandardReports))*len(str(output_data))))
                logger.info("## EVT:TRUNC_ML_METADATA ## Metadata prompt is "+str(promptTokensConsumed_allCustomReportsAndStandardReports)+" and needs reduction of full output length "+str(len(str(output_data)))+" of "+str(charReduce)+" to meet max of "+str(maxTokensAvailLLM_allCustomReportsAndStandardReports))
                output_data = (str(output_data)[:charReduce]+"...\n\n###\nRemaining "+str(charReduce)+" characters of popular data source and field list was truncated to fit LLM Gateway API token limits")
                #return output_data
            else:
                logger.info("## INFO:TOKEN_NOT_EXCEED ## No truncation required of allCustomReportsAndStandardReports due to "+str(type(output_data))+"of token length "+str(promptTokensConsumed_allCustomReportsAndStandardReports)+" within max of "+str(maxTokensAvailLLM_allCustomReportsAndStandardReports))
                extraTokensAvailLLM += maxTokensAvailLLM_allCustomReportsAndStandardReports-promptTokensConsumed_allCustomReportsAndStandardReports
                output_data = str(output_data)

            s3_client = boto3.client("s3")
            S3_BUCKET = BUCKET_NAME #'workday-wcp-ffmmnb-new-us-west-2'

            object_key = OBJECT_NAME6 #"einsteinGPTPromptBuilder/jobProfiles_MgmtCompGrades.json"  # replace object key
            logger.info('## INFO:S3_OPERATION_START ## Starting to retrieve S3 object '+object_key)    
            try:
                file_content = s3_client.get_object(
                    Bucket=S3_BUCKET, Key=object_key)["Body"].read()
            except s3_client.exceptions.NoSuchKey:
                logger.warning("Unknown error when trying to retrieve WD metadata NoSuchKey (potential INT0294 staging long term or S3 lifecycle missing short term failure) on file: %s", object_key)
                file_content = {"lambda_errors": [
                  {
                  "reportLibrarylambda-amplify-einsteinRepLibMetaCard-DEV-ERR-003M09: Unexpected error when trying to retrieve WD metadata not NoSuchKey for "+object_key+" in aws_request_id "+ jsonpickle.encode(str(context.aws_request_id)) +" at "+ str(dt.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f%Z"))
                  }
                ]}
                return file_content
            except s3_client.exceptions.ClientError as e:            
                logging.error(traceback.format_exc())
                logger.warning("Unknown error when trying to retrieve WD metadata not NoSuchKey: %s", object_key)
                file_content = {"lambda_errors": [
                  {
                  "reportLibrarylambda-amplify-einsteinRepLibMetaCard-DEV-ERR-003M10: Unexpected error when trying to retrieve WD metadata not NoSuchKey for "+object_key+" in aws_request_id "+ jsonpickle.encode(str(context.aws_request_id)) +" at "+ str(dt.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f%Z"))
                  }
                ]}
                return file_content

            logger.info(('## Lambda jobProfiles_MgmtCompGrades file length was '+str(len(file_content))))#+' and token use would be '+str(num_tokens_from_string(str(file_content), tiktoken_encoding)))
            #token_fulluse_total += num_tokens_from_string(str(file_content), tiktoken_encoding)
            #print(file_content)
            read_data = file_content
            jobProfileCompMgmtGradeData = json.loads(read_data)
            #read_data = data_stream.read() ###was f.read()

            #####data = json.loads(read_data)
            jobProfCompMgmtGradeData_output_json = {
                "jobProfiles": [],
                "total": jobProfileCompMgmtGradeData["total"]
            }
            # Sort the data in ascending order based on "workerCountWithJobProfile", defaulting to 0 if not present
            sorted_data_workerCountWithJobProfile = sorted(jobProfileCompMgmtGradeData["data"], key=lambda x: x.get("workerCountWithJobProfile", 0), reverse=True)
            for data in sorted_data_workerCountWithJobProfile:
                jobProfCompMgmtGradeData_output_json["jobProfiles"].append({
                    #"workdayID": data["workdayID"],
                    "jobProfileName": data["jobProfileName"],
                    #"ID": data["ID"],
                    #"workerCountWithJobProfile": data["workerCountWithJobProfile"] if "workerCountWithJobProfile" in data else None,
                    "jobFamily": data["jobFamily"] if "jobFamily" in data else None,
                    "jobFamilyGroup": data["jobFamilyGroup"] if "jobFamilyGroup" in data else None,
                    "managementLevel": data["managementLevel"] if "managementLevel" in data else None,
                    "compensationGrade": data["compensationGrade"] if "compensationGrade" in data else None
                })
            promptTokensConsumed_jobProfiles_MgmtCompGrades = num_tokens_from_string(str(jobProfCompMgmtGradeData_output_json), tiktoken_encoding, context)
            if extraTokensAvailLLM > 0:
                logger.info("## INFO:TOKEN_COUNT ## Adding extra tokens of "+str(extraTokensAvailLLM)+" to normal maxTokensAvailLLM_jobProfiles_MgmtCompGrades avail if "+str(maxTokensAvailLLM_jobProfiles_MgmtCompGrades)+" to equal "+str(maxTokensAvailLLM_jobProfiles_MgmtCompGrades+extraTokensAvailLLM))
                maxTokensAvailLLM_jobProfiles_MgmtCompGrades += extraTokensAvailLLM
            else:
                logger.info("## INFO:TOKEN_COUNT ## No extra tokens to allocate remaining maxTokensAvailLLM_jobProfiles_MgmtCompGrades avail of "+str(maxTokensAvailLLM_jobProfiles_MgmtCompGrades))            
            logger.info("## INFO:TOKEN_COUNT ## Tokens consumed by jobProfiles_MgmtCompGrades before truncation is "+str(promptTokensConsumed_jobProfiles_MgmtCompGrades)+" versus of max tokens "+str(maxTokensAvailLLM_jobProfiles_MgmtCompGrades)+" out of entire prompt limit of "+str(event_libcard_prompt_length_max))
            if promptTokensConsumed_jobProfiles_MgmtCompGrades > maxTokensAvailLLM_jobProfiles_MgmtCompGrades: 
                charReduce=int((((maxTokensAvailLLM_jobProfiles_MgmtCompGrades/promptTokensConsumed_jobProfiles_MgmtCompGrades))*len(str(jobProfCompMgmtGradeData_output_json))))
                logger.info("## EVT:TRUNC_ML_METADATA ## jobProfiles_MgmtCompGrades prompt is "+str(promptTokensConsumed_jobProfiles_MgmtCompGrades)+" and needs reduction of full output length "+str(len(str(jobProfCompMgmtGradeData_output_json)))+" of "+str(charReduce)+" to meet max of "+str(maxTokensAvailLLM_jobProfiles_MgmtCompGrades))
                jobProfCompMgmtGradeData_output_json = (str(jobProfCompMgmtGradeData_output_json)[:charReduce]+"...\n\n###\nRemaining "+str(charReduce)+" characters of jobProfiles_MgmtCompGrades was truncated to fit LLM Gateway API token limits")
                #return output_data
            else:
                logger.info("## INFO:TOKEN_NOT_EXCEED ## No truncation required of jobProfiles_MgmtCompGrades due to "+str(type(jobProfCompMgmtGradeData_output_json))+"of token length "+str(promptTokensConsumed_jobProfiles_MgmtCompGrades)+" within max of "+str(maxTokensAvailLLM_jobProfiles_MgmtCompGrades))
                extraTokensAvailLLM = maxTokensAvailLLM_jobProfiles_MgmtCompGrades-promptTokensConsumed_jobProfiles_MgmtCompGrades
                jobProfCompMgmtGradeData_output_json = str(jobProfCompMgmtGradeData_output_json)

            '''
            s3_client2 = boto3.client("s3")
            S3_BUCKET = BUCKET_NAME #'workday-wcp-ffmmnb-new-us-west-2'

            object_key = sf_einstein_base_prompt #"einsteinGPTPromptBuilder/"  # replace object key
            try:
                file_content = s3_client2.get_object(
                    Bucket=S3_BUCKET, Key=object_key)["Body"].read()
            except s3_client2.exceptions.NoSuchKey:
                logger.warning("Unknown error when trying to retrieve basePromptStatic NoSuchKey (potential S3 lifecycle deletion missing short term failure) on file: %s", object_key)
                file_content = {"lambda_errors": [
                  {
                  "reportLibrarylambda-amplify-einsteinRepLibMetaCard-DEV-ERR-003M11A: Unexpected error when trying to retrieve basePromptStatic not NoSuchKey for "+object_key+" in aws_request_id "+ jsonpickle.encode(str(context.aws_request_id)) +" at "+ str(dt.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f%Z"))
                  }
                ]}
                return file_content
            except s3_client2.exceptions.ClientError as e:            
                logging.error(traceback.format_exc())
                logger.warning("Unknown error when trying to retrieve basePromptStatic not NoSuchKey: %s", object_key)
                file_content = {"lambda_errors": [
                  {
                  "reportLibrarylambda-amplify-einsteinRepLibMetaCard-DEV-ERR-003M12A: Unexpected error when trying to retrieve basePromptStatic not NoSuchKey for "+object_key+" in aws_request_id "+ jsonpickle.encode(str(context.aws_request_id)) +" at "+ str(dt.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f%Z"))
                  }
                ]}
                return file_content
                '''

            #print(file_content)
            #print(filtered_prompt.encode('utf-8'))
            read_data = file_content+filtered_prompt.encode('utf-8')
            todays_date = dt.today()
            logger.info("## INFO:TODAY ## Current date is "+str(todays_date.strftime('%Y-%m-%d')))
            file_content = filtered_prompt#.decode('utf-8')
            #file_content.replace('|TODAY|'.encode('ascii'), str(todays_date).encode('ascii'))
            #file_content.replace('|TODAY_PLUS_90D|'.encode('ascii'), str(todays_date).encode('ascii') + str(datetime.timedelta(days=10)).encode('ascii'))
            file_content = file_content.replace('|TODAY|', str(todays_date.strftime('%Y-%m-%d')))
            file_content = file_content.replace('|TODAY_YY|', str(todays_date.strftime('%Y')))
            file_content = file_content.replace('|TODAY_MM|', str(todays_date.strftime('%m')))
            file_content = file_content.replace('|TODAY_DD|', str(todays_date.strftime('%d')))
            file_content = file_content.replace('|WD_USERNAME|', str(wd_username))
            todays_date_plus_90d = todays_date + timedelta(days=90)
            logger.info("## INFO:TODAY ## Current date plus 90 days is "+str(todays_date_plus_90d.strftime('%Y-%m-%d')))
            file_content = file_content.replace('|TODAY_PLUS_90D|', str(todays_date_plus_90d.strftime('%Y-%m-%d')))
            file_content = file_content.encode('utf-8')
            file_content += '|'.encode('utf-8')+"\n".encode('utf-8')
            logger.info("## INFO:TOKEN_COUNT_DRAFT1 ## Length of draft basePromptStatic output is "+str(len(file_content)))#+" and tokens consumed are "+str(num_tokens_from_string(str(file_content), tiktoken_encoding)))
            file_content += '\"\"\"\n###\n\nFor additional context, see just following three metadata groups of JSON output called locationsUsedAsBusinessSite supervisoryOrganizations AND allCustomReportsAndStandardReports.  These three sets of data represent key information for you to interpret the user desired user population described below to create a query with proper syntax and making viable assumptions.\"\"\"\n###\n\nThe first JSON is locationsUsedAsBusinessSite which has keys of note such as location with location names, primaryAddress_FullWithCountry with address of the location. Please weight your consideration of similar locations based on their order which is by locationWorkerCount descending indicating how common and useful the locations are when the user input is unclear.\"\"\"\n###\n\nJSON 01 - locationsUsedAsBusinessSite\n\n'.encode('ascii')
            logger.info("## INFO:TOKEN_COUNT_DRAFT2 ## Length of draft locationsUsedAsBusinessSite output is "+str(len(locData_output_json)))#+" and tokens consumed are "+str(num_tokens_from_string(str(locData_output_json), tiktoken_encoding)))
            file_content += bytes(str(locData_output_json), 'utf-8')
            file_content += '\"\"\"\n###\n\nThe second JSON is supervisoryOrganizations and contains the most important departments or supervisory organizations with supervisoryManager attributes indicating the person in charge of that group.  Other important keys include defaultOrganizationAssignmentsPrimaryOrgTypes that lists the default cost center, company and other values associated with this level of the company. The key businessTitle beneath supervisoryManager can indicate the title of the leader of that group and jobFamily and jobFamilyGroup keys in the same level indicate what type of work is done there. The field employeeCount_IncludeSubordinateOrganizationsIndexed indicates how many total employees and other workers sit beneath this level of the organization. Please weight your response tending towards the largest organizations via employeeCount_IncludeSubordinateOrganizationsIndexed.\"\"\"\n###\n\nJSON 02 - supervisoryOrganizations\n\n'.encode('ascii')
            logger.info("## INFO:TOKEN_COUNT_DRAFT3 ## Length of draft supervisoryOrganizations output is "+str(len(supOrgData_output_json)))#+" and tokens consumed are "+str(num_tokens_from_string(str(supOrgData_output_json), tiktoken_encoding)))
            file_content += bytes(str(supOrgData_output_json), 'utf-8')
            file_content += '\"\"\"\n###\n\nThe third JSON is allActiveJobProfiles and contains the the types of jobs and roles that workers at Salesforce can occupy with workdayID being the method to query allWorkers and other data sources on the field jobProfile.  Other important fields include the jobProfileName as well as the jobFamily and jobFamilyGroup (which categorize the work done and can be filtered using the id on those via the specific noted job profile/family/family group text filter or with WID other fields on allWorkers and such respectively). The field workerCountWithJobProfile indicates how many total employees and other workers are assigned to this job profile. Please weight your response tending towards the most common job profiles via workerCountWithJobProfile.\"\"\"\n###\n\nJSON 03 - allActiveJobProfiles\n\n'.encode('ascii')
            logger.info("## INFO:TOKEN_COUNT_DRAFT4 ## Length of draft jobProfiles_MgmtCompGrades output is "+str(len(jobProfCompMgmtGradeData_output_json)))#+" and tokens consumed are "+str(num_tokens_from_string(str(jobProfCompMgmtGradeData_output_json), tiktoken_encoding)))
            file_content += bytes(str(jobProfCompMgmtGradeData_output_json), 'utf-8')
            file_content += '\"\"\"\n###\n\nThe final JSON is allCustomReportsAndStandardReports and contains the entire list of relevant fields within fieldsDisplayedOnReport each as WQLAlias (to be used in SELECT and WHERE) having count as the number of times existing reports used these fields and data sources within dataSource__WQLAlias with numberOfTimesExecuted indicating the number of times any report using that data source was run. Please weight your response tending towards the most popular fields and data sources indicated here and if you encounter \'...\' instead of a valid JSON end it has been tuncated to meet token length limitations of LLM Gateway API.\"\"\"\n###\n\nJSON 03 - allCustomReportsAndStandardReports\n\n'.encode('ascii')
            logger.info("## INFO:TOKEN_COUNT_DRAFT5 ## Length of draft allCustomReportsAndStandardReports output is "+str(len(output_data)))#+" and tokens consumed are "+str(num_tokens_from_string(str(output_data), tiktoken_encoding)))
            file_content += bytes(output_data, 'utf-8')
            file_content += bytes("\"\"\"\n###\n\nThe desired population has been described by the user in the following way:\n", 'utf-8')

            logger.info("## INFO:TOKEN_COUNT_OUTPUT ## Length of FINAL RESPONSE OUTPUT is "+str(len(file_content))+" and tokens consumed are "+str(num_tokens_from_string(str(file_content), tiktoken_encoding, context)))
            return file_content
        else:
            logger.info(('## Present mock wait time was ') + str(event_mock_wait_time))
            try:
                if event_mock_wait_time.is_integer():
                    time.sleep(int(event_mock_wait_time)/1000)
                else:
                    logger.info(('## Not mock time to wait needed'))
            except Exception as e:
                logging.error(traceback.format_exc())
            s3_client2 = boto3.client("s3")
            S3_BUCKET = BUCKET_NAME #'workday-wcp-ffmmnb-new-us-west-2'

            object_key = sf_einstein_base_prompt #"einsteinGPTPromptBuilder/"  # replace object key
            try:
                file_content = s3_client2.get_object(
                    Bucket=S3_BUCKET, Key=object_key)["Body"].read()
            except s3_client2.exceptions.NoSuchKey:
                logger.warning("Unknown error when trying to retrieve basePromptStatic NoSuchKey (potential S3 lifecycle deletion missing short term failure) on file: %s", object_key)
                file_content = {"lambda_errors": [
                  {
                  "reportLibrarylambda-amplify-einsteinRepLibMetaCard-DEV-ERR-003M11B: Unexpected error when trying to retrieve basePromptStatic not NoSuchKey for "+object_key+" in aws_request_id "+ jsonpickle.encode(str(context.aws_request_id)) +" at "+ str(dt.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f%Z"))
                  }
                ]}
                return file_content
            except s3_client2.exceptions.ClientError as e:            
                logging.error(traceback.format_exc())
                logger.warning("Unknown error when trying to retrieve basePromptStatic not NoSuchKey: %s", object_key)
                file_content = {"lambda_errors": [
                  {
                  "reportLibrarylambda-amplify-einsteinRepLibMetaCard-DEV-ERR-003M12B: Unexpected error when trying to retrieve basePromptStatic not NoSuchKey for "+object_key+" in aws_request_id "+ jsonpickle.encode(str(context.aws_request_id)) +" at "+ str(dt.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f%Z"))
                  }
                ]}
                return file_content
            print(file_content)
            read_data = file_content
            file_content += '|'.encode('ascii')+"\n".encode('ascii')
            read_data = file_content
    else:
        logger.error(f"Unsupported operation: {wd_ext_operation}")
        file_content = {"lambda_errors": [
      {
      "reportLibrarylambda-amplify-einsteinRepLibMetaCard-DEV-ERR-003: Invalid wd_ext_operation with value "+str(wd_ext_operation)+" in aws_request_id "+ jsonpickle.encode(str(context.aws_request_id)) +" at "+ str(dt.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f%Z"))
      }
    ]}
    return file_content
    #return json.dumps(WD_EXT_OPERATION, indent=4)


def return_recent_user_queries(bucket_name, wd_tenant_alias, wd_username, context):
    import boto3
    import json
    import pandas as pd
    from io import StringIO
    import logging
    global file_content
    s3_client3 = boto3.client("s3")
    #S3_BUCKET = BUCKET_NAME #'workday-wcp-ffmmnb-new-us-west-2'

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.info("Starting execution of return_recent_user_queries")
    report_lib_user_queries = 'einsteinGPTPromptBuilder/'+str(wd_tenant_alias)+'/user_queries/'+str(wd_username)+'-reportLibraryMetadataPromptUserGenCard_queries.json'


    object_key_queries = report_lib_user_queries #"einsteinGPTPromptBuilder/"  # replace object key
    try:
        #obj_for_user_queries_response = s3_client3.get_object(Bucket=bucket_name, Key=object_key_queries)
        file_content = json.loads(s3_client3.get_object(Bucket=bucket_name, Key=object_key_queries)['Body'].read().decode('utf-8'))
        print(json.dumps(file_content, indent=4))
        try:
            logger.warning("Existing User Query file returned from obj is:"+ str(file_content))
            #read_data = file_content
        except s3_client3.exceptions.ClientError as e:            
            logging.error(traceback.format_exc())
            logger.warning("Unknown error when trying to retrieve user_queries not NoSuchKey: %s", object_key_queries)
            file_content = {"lambda_errors": [
              {
              "reportLibrarylambda-amplify-einsteinRepLibMetaCard-DEV-ERR-003A1: Unexpected error when trying to retrieve user_queries not NoSuchKey for "+object_key_queries+" in aws_request_id "+ jsonpickle.encode(str(context.aws_request_id)) +" at "+ str(dt.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f%Z"))
              }
            ]}
            return file_content
    except s3_client3.exceptions.NoSuchKey:
        logger.info(('No recent queries file for username following baseline generating ') + str(wd_username))# + e.response['Error']['Code'])
        base_user_queries = str('{"recentQueries":{"wd_username":"'+wd_username+'","addedDate":"'+str(date.today())+'","lastPrunedDate":null,"createdDate":"'+str(date.today())+'","lastUpdated":"'+str(date.today())+'","totalQueriesforUserPrevRemoved":null,"totalQueryRunsforUserInclRemoved":null,"queryList":{"reportLibraryMetadataPromptUserGenCard_queries":[]}}}')
        #print(json.dumps(base_user_queries, indent=4))
        try:
            result_s3_write = s3_client3.put_object(Body=bytes(base_user_queries, 'utf-8'), Bucket=bucket_name, Key=object_key_queries)
            logger.info(('## S3 put_object HTTPStatusCode is ') +str(result_s3_write['ResponseMetadata']['HTTPStatusCode'])+' and x-amz-server-side-encryption is '+str(result_s3_write['ResponseMetadata']['HTTPHeaders']['x-amz-server-side-encryption'])+' and x-amz-request-id is '+str(result_s3_write['ResponseMetadata']['HTTPHeaders']['x-amz-request-id']))
            #file_content=json.load(result_s3_write, indent=4)
            file_content=json.loads(bytes("{}",'utf-8'))
        except s3_client3.exceptions.ClientError as e:            
            logging.error(traceback.format_exc())
            logger.warning("Unknown error when trying to write user_queries new file: %s", object_key_queries)
            file_content = {"lambda_errors": [
              {
              "reportLibrarylambda-amplify-einsteinRepLibMetaCard-DEV-ERR-003A2: Unexpected error when trying to write user_queries new file at "+bucket_name+object_key_queries+" in aws_request_id "+ jsonpickle.encode(str(context.aws_request_id)) +" at "+ str(dt.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f%Z"))
              }
            ]}
            return file_content
    except Exception as e:
        logging.error(traceback.format_exc())
        logger.warning("Unknown other error when trying to retrieve user_queries file: %s", object_key_queries)
        file_content = {"lambda_errors": [
          {
          "reportLibrarylambda-amplify-einsteinRepLibMetaCard-DEV-ERR-003A3: Unexpected other error when trying to retrieve user_queries not NoSuchKey for "+object_key_queries+" in aws_request_id "+ jsonpickle.encode(str(context.aws_request_id)) +" at "+ str(dt.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f%Z"))
          }
        ]}
        return file_content
    logger.info("Finished execution of return_recent_user_queries")#+str(context.aws_request_id))
    file_content["aws_request_id"] = str(context.aws_request_id)

'''    try:
        file_content += str(s3_client3.get_object(
            Bucket=bucket_name, Key=object_key_queries)["Body"].read())
        logger.warning("Read recent queries value is: %s", file_content)
        logger.info(('## EVT:NEW_QUERIES_USER ## ')+str(wd_username)+' >> '+str(bucket_name)+' key '+str(object_key_queries))
        file_content += str(base_user_queries)
        read_data = file_content
    except Exception as e:
        logging.error(traceback.format_exc())
        logger.info(('No recent queries file for username following baseline generating ') + str(wd_username))# + e.response['Error']['Code'])
        base_user_queries = str('{"recentQueries":{"wd_username":wd_username,"addedDate":'+str(date.today())+',"lastPrunedDate":null,createdDate":'+str(date.today())+',"lastUpdated":'+str(date.today())+',"totalQueriesforUserPrevRemoved":null,"totalQueryRunsforUserInclRemoved":null,"queryList":{"reportLibraryMetadataPromptUserGenCard_queries":[]}}}')
        try:
            result_s3_write = s3_client3.put_object(Body=bytes(base_user_queries, 'utf-8'), Bucket=bucket_name, Key=object_key_queries)
            logger.info(('## S3 put_object HTTPStatusCode is ') +str(result_s3_write['ResponseMetadata']['HTTPStatusCode'])+' and x-amz-server-side-encryption is '+str(result_s3_write['ResponseMetadata']['HTTPHeaders']['x-amz-server-side-encryption'])+' and x-amz-request-id is '+str(result_s3_write['ResponseMetadata']['HTTPHeaders']['x-amz-request-id']))
        except Exception as e:
            logging.error(traceback.format_exc())'''
            
def get_allowed_wql_aliases(user_wids, role_wids, all_data_sources, assignable_roles):
    # Map roleWID -> [securityGroupWID]
    role_to_secgroups = {
        entry["workdayID"]: [sg["id"] for sg in entry.get("securityGroups", [])]
        for entry in assignable_roles["data"]
    }
    
    # Expand roles to security groups
    role_secgroups = set()
    for wid in role_wids:
        role_secgroups.update(role_to_secgroups.get(wid, []))
    logger.info(f"## INFO:ROLE_SECGROUP_LIST ## Role_to_secgroups {role_secgroups}")
    # Combine user-based and role-based security groups
    total_secgroups = set(user_wids) | role_secgroups
    logger.info(f"## INFO:ALL_SECGROUP_LIST ## Total_secgroups {total_secgroups}")

    # Identify allowed WQLAliases
    allowed_aliases = set()
    for ds_entry in all_data_sources["data"]:
        alias = ds_entry.get("WQLAlias")
        secgroup_ids = [sg["id"] for sg in ds_entry.get("securityGroups1", [])]
        # Log security group WIDs for this alias
        #logger.info(f"WQLAlias: {alias} → securityGroups1 WIDs: {secgroup_ids}")
        if alias and total_secgroups & set(secgroup_ids):
            allowed_aliases.add(alias)
    
    # Fallback for IC users
    if "allWorkers" not in allowed_aliases:
        logger.info("'## INFO:allWorkers_SUB_NEEDED_BASE_SEC ## allWorkers' not found in allowed_aliases. Adding it for fallback.")
        allowed_aliases.add("allWorkers")

    return allowed_aliases



def generate_filtered_prompt(prompt_lines, allowed_aliases):
    final_lines = []
    i = 0
    n = len(prompt_lines)

    while i < n:
        line = prompt_lines[i]

        if line.startswith("|SEC-CHECK__"):
            alias = line.split("|")[1].replace("SEC-CHECK__", "")
            is_allowed = alias in allowed_aliases

            if is_allowed:
                # Include this line (cleaned)
                final_lines.append(line.split("|", 2)[2])
                i += 1
                # Skip all following |SEC-FREE-ALT| lines
                while i < n and prompt_lines[i].startswith("|SEC-FREE-ALT|"):
                    i += 1
            else:
                # Skip this line
                i += 1
                # Include all following |SEC-FREE-ALT| lines (cleaned)
                while i < n and prompt_lines[i].startswith("|SEC-FREE-ALT|"):
                    final_lines.append(prompt_lines[i].split("|", 2)[2])
                    i += 1
        else:
            # Normal line, just include
            final_lines.append(line)
            i += 1

    return "\n".join(final_lines)



def num_tokens_from_string(string: str, encoding_name: str, context) -> int:
    try:
        encoding = tiktoken.get_encoding(encoding_name)
    except Exception as e:
        logger.error("Error getting tiktoken encoding in %s: %s", encoding_name, str(e))
        file_content = str({"lambda_errors": [
            {
            "reportLibrarylambda-amplify-einsteinRepLibMetaCard-DEV-ERR-007A: Error trying to load token measurement encoding "+encoding_name+" due to "+str(e)+" in aws_request_id "+ jsonpickle.encode(str(context.aws_request_id)) +" at "+ str(dt.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f%Z"))
            }
        ]})
        return file_content
    try:
        num_tokens = len(encoding.encode(string))
    except Exception as e:
        logger.error("Error parsing JSON in %s: %s", file_key, str(e))
        file_content = str({"lambda_errors": [
            {
            "reportLibrarylambda-amplify-einsteinRepLibMetaCard-DEV-ERR-007B: Error measuring token usage due to "+str(e)+" in aws_request_id "+ jsonpickle.encode(str(context.aws_request_id)) +" at "+ str(dt.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f%Z"))
            }
        ]})
        return file_content
    return num_tokens

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# validate
if os.path.exists(os.path.join(tiktoken_cache_dir, tiktoken_cache_filename)):
    logger.info('## INFO:TOKEN_CACHE_FOUND ## Found toktoken ML token length cached file '+tiktoken_cache_filename+' of size '+str(os.path.getsize(os.path.join(tiktoken_cache_dir, tiktoken_cache_filename)))+' due to inability to access network in WD Extend provisioned AWS Lambda')
    encoding = tiktoken.get_encoding(tiktoken_encoding) 
    logger.info('## INFO:TOKEN_CACHE_TEST ## Example token test is '+str(encoding.encode("Hello, world"))+' using encoding name of '+str(tiktoken_encoding))

logger.info('## INFO:TOKEN_ENCODING_NAME ## Using encoding name of '+str(tiktoken_encoding))

client = boto3.client('lambda')

def flatten_json(y, prefix=''):
    out = {}

    def flatten(x, name=''):
        if isinstance(x, dict):
            for a in x:
                flatten(x[a], f"{name}{a}_")
        elif isinstance(x, list):
            for i, a in enumerate(x):
                flatten(a, f"{name}{i}_")
        else:
            out[name[:-1]] = x

    flatten(y, prefix)
    return out

def process_all_json_to_csv_and_upload(bucket_name, base_prefix, event, context):
    import boto3
    import json
    import pandas as pd
    from io import StringIO, BytesIO 
    import logging
    global file_content

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.info("Starting execution of process_all_json_to_csv_and_upload")

    s3_client = boto3.client("s3")

    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=base_prefix)
    json_files = sorted([obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith(".json")])

    if not json_files:
        logger.warning("No JSON files found under prefix: %s", base_prefix)
        file_content = {"lambda_errors": [
          {
          "reportLibrarylambda-amplify-einsteinRepLibMetaCard-DEV-ERR-003B: Unable to find JSON files in operation process_all_json_to_csv_and_upload given uuid of "+event.get("uuid")+" in aws_request_id "+ jsonpickle.encode(str(context.aws_request_id)) +" at "+ str(dt.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f%Z"))
          }
        ]}
        return file_content

    CHUNK_ROWS = 20_000          # 🔋 write every N rows instead of whole DF in RAM
    first_keys, buffer_rows, csv_stream = [], [], StringIO()

    all_data = []
    first_keys = []

    for file_key in json_files:
        logger.info("Reading file: %s", file_key)
        file_content = s3_client.get_object(Bucket=bucket_name, Key=file_key)["Body"].read().decode("utf-8")

        # Strip leading invisible Unicode chars
        file_content = re.sub(r'^[\ufeff\u200b\u00a0]+', '', file_content.strip())

        try:
            raw_data = json.loads(file_content)
        except json.JSONDecodeError as e:
            logger.error("Error parsing JSON in %s: %s", file_key, str(e))
            file_content = str({"lambda_errors": [
                {
                "reportLibrarylambda-amplify-einsteinRepLibMetaCard-DEV-ERR-004: Error parsing JSON "+str(e)+" in aws_request_id "+ jsonpickle.encode(str(context.aws_request_id)) +" at "+ str(dt.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f%Z"))
                }
            ]})
            return file_content

        # If JSON is a dict with a key like "data", extract the array
        # Determine structure of JSON
        if isinstance(raw_data, dict) and "data" in raw_data and isinstance(raw_data["data"], list):
            data = raw_data["data"]
            if not data:
                logger.info("File %s has empty 'data' list, skipping row addition", file_key)
                continue
        elif isinstance(raw_data, list):
            data = raw_data
        elif isinstance(raw_data, dict):
            data = [raw_data]
        else:
            logger.warning("Skipping file %s due to unexpected structure: type=%s", file_key, type(raw_data))
            continue

        for item in data:
            if not isinstance(item, dict):
                continue
            flat_item = flatten_json(item)
            if not first_keys:
                first_keys = list(flat_item.keys())
            all_data.append(flat_item)

    if not all_data:
        logger.warning("No valid JSON objects found under prefix: %s", base_prefix)
        return

# Create DataFrame and ensure column order
    df = DataFrame(all_data)
    df = df.reindex(columns=first_keys + [col for col in df.columns if col not in first_keys])
    # Exclude columns ending with '_id'
    df_filtered = df[[col for col in df.columns if not col.endswith('_id')]]#new change

    # List of abbreviations to preserve (add more as needed)
    abbreviations = {"ID"} #if any new abbrevations in functure we can add here like SSN, DOB etc.

    formatted_columns = []
    for col in df_filtered.columns:
        if "descriptor" in col.lower():
            # Remove 'descriptor' and digits
            col_clean = re.sub(r'\d+', '', col, flags=re.IGNORECASE)
            col_clean = re.sub(r'descriptor', '', col_clean, flags=re.IGNORECASE).strip()
            col_clean = col_clean.split()[0]
        else:
            col_clean = col

        # Remove underscores
        col_clean = col_clean.replace('_', '')

        # Split camelCase into words
        words = re.findall(r'[A-Z]+(?=[A-Z][a-z])|[A-Z]?[a-z]+|[A-Z]+', col_clean)

        # Capitalize normally unless it's an abbreviation
        pretty_words = [word if word.upper() in abbreviations else word.capitalize() for word in words]

        col_final = ' '.join(pretty_words)
        formatted_columns.append(col_final)
    
    # Write to CSV in memory
    df_filtered.columns = formatted_columns
    csv_output = StringIO()
    # ----- Compose Custom Header Lines -----
    header_lines = [
        "Salesforce Agentforce",
        "WD Report Library",
        f"{event.get('wd_username', 'unknown')} {event.get('wd_tenant_alias', 'unknown')}",
        f"_{dt.now(timezone.utc).strftime('%Y-%m-%d')}",
        f"{dt.now(timezone.utc).strftime('%H:%M:%S')} UTC",
        #event.get("uuid", "unknown"),
        #event.get("uuid", "unknown") if len(event.get("uuid", "unknown")) < 10 else f"{event.get('uuid', 'unknown')[:8]},{event.get('uuid', 'unknown')[8:13]},{event.get('uuid', 'unknown')[13:18]},{event.get('uuid', 'unknown')[18:]}"
        event.get("uuid", "unknown") if len(event.get("uuid", "unknown")) < 10 else "\""+str(event.get('uuid', 'unknown')[:8])+"\n"+str(event.get('uuid', 'unknown')[8:13])+"\n"+str(event.get('uuid', 'unknown')[13:18])+"\n"+str(event.get('uuid', 'unknown')[18:]) +"\"" + "\n"
        "",  # blank line
        "",  # blank line
    ]
    header_str = "\n".join(header_lines) + "\n"
    # --------------------------------------
    try:
        df_filtered.to_csv(
            csv_output,
            index=False,
            quoting=1,
            lineterminator='\r',
            #header=formatted_columns
        )
        #csv_bytes = csv_output.getvalue().encode("utf-8")
        final_csv_with_header = header_str + csv_output.getvalue()
        csv_bytes = final_csv_with_header.encode("utf-8")
        

    except Exception as e:
        logging.error(traceback.format_exc())
        logger.warning("Error while converting DataFrame to CSV for file %s", file_key)
        file_content = {"lambda_errors": [
        {
            "reportLibrarylambda-amplify-einsteinRepLibMetaCard-DEV-ERR-003M04F: Error converting DataFrame to CSV for " + file_key + " in aws_request_id " + jsonpickle.encode(str(context.aws_request_id)) + " at " + str(dt.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f%Z"))
        }
        ]}
        return file_content

    output_filename = base_prefix + "combined_output.csv"
    s3_client.put_object(
        Bucket=bucket_name,
        Key=output_filename,
        Body=csv_bytes
    )
    logger.info("CSV written to S3 at: %s", output_filename)

    try:
        file_content = json.loads(file_content)
    except json.JSONDecodeError as e:
        logger.error("Error parsing JSON in %s: %s", file_key, str(e))
        file_content = str({"lambda_errors": [
            {
            "reportLibrarylambda-amplify-einsteinRepLibMetaCard-DEV-ERR-004B: Error parsing JSON "+str(e)+" in aws_request_id "+ jsonpickle.encode(str(context.aws_request_id)) +" at "+ str(dt.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f%Z"))
            }
        ]})
        return file_content

    file_content = {"files": [
      {
        "file_name": str(output_filename),
        "file_size": len(csv_bytes)
      }
    ], "aws_request_id": str(context.aws_request_id)}
    logger.info("Finished execution of process_all_json_to_csv_and_upload")
        
def delete_combined_output_csv_file(bucket_name, uuid_folder_prefix, context):
    import boto3
    import logging
    global file_content
    file_content = json.loads("{}")
    file_content = {"files": [], "aws_request_id": str(context.aws_request_id)}

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    s3_client = boto3.client("s3")
    logger.info("Starting execution of process_delete_temp_files")

    try:
        obj_for_del_response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=uuid_folder_prefix)
        logger.warning("Temp files returned from list obj: %s", obj_for_del_response['Contents'])
        try:
            for object in obj_for_del_response['Contents']:
                logger.info("Deleting detected file: %s", object['Key'])
                s3_client.delete_object(Bucket=bucket_name, Key=object['Key']) 
                file_content["files"].append((object['Key'], object['Size']))
        except s3_client.exceptions.ClientError as e:
            logger.warning("Temp file for deletion not found (maybe already deleted): %s", object['Key'])
    except s3_client.exceptions.ClientError as e:
        if e.obj_for_del_response['Error']['Code'] == "404":
            logger.warning("No objects with prefix found: %s", uuid_folder_prefix)
        else:
            logger.error("Error checking/deleting %s: %s", uuid_folder_prefix, str(e))
            file_content = {"lambda_errors": [
            {
            "reportLibrarylambda-amplify-einsteinRepLibMetaCard-DEV-ERR-005: Error checking/deleting uuid_folder_prefix "+str(e)+" in aws_request_id "+ jsonpickle.encode(str(context.aws_request_id)) +" at "+ str(dt.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f%Z"))
            }]}
            return file_content
    except Exception as e:
        logger.error("Unexpected error while deleting temp files: %s", str(e))
        file_content = {"lambda_errors": [
        {
        "reportLibrarylambda-amplify-einsteinRepLibMetaCard-DEV-ERR-006: Unexpected error while deleting temp files "+str(e)+" in aws_request_id "+ jsonpickle.encode(str(context.aws_request_id)) +" at "+ str(dt.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f%Z"))
        }]}
        return file_content
    logger.info("Finished execution of process_delete_temp_files")

def load_json_from_s3(bucket, key):
    try:
        logger.info(f"Attempting to load S3 object: {key} from bucket: {bucket}")
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        return json.loads(content)
    except s3_client.exceptions.NoSuchKey:
        logger.warning(f"S3 object not found: {key}")
        raise FileNotFoundError(f"File not found: {key}")
    except Exception as e:
        logger.error(traceback.format_exc())
        raise RuntimeError(f"Failed to load file from S3: {str(e)}")
    
def handle_agentforce_preview_data_pull(event, context):
    global file_content
    bucket_name = str(event.get('_aws_s3_bucket', 'workday-wcp-ffmmnb-new-us-west-2'))
    uuid = event.get("uuid")
    wd_tenant_alias = event.get("wd_tenant_alias")
    # mock_wait_time = int(event.get("mock_wait_time", 0))  # Optional retry pause

    if not uuid or not wd_tenant_alias:
        file_content = {"lambda_errors": [
            {
              f"reportLibrarylambda-amplify-einsteinRepLibMetaCard-DEV-ERR-007: Missing uuid or wd_tenant_alias in agentforcePreviewDataPull. aws_request_id {jsonpickle.encode(str(context.aws_request_id))} at {dt.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f%Z')}"
            }
        ]}
        return file_content

    s3_key = f"einsteinGPTPromptBuilder/{wd_tenant_alias}/tmp_preview_data_pull/{uuid}/00.json"
    logger.info(f"Looking for preview file at: {s3_key}")

    try:
        # Optional wait logic (commented out for now)
        # if mock_wait_time > 0:
        #     logger.info(f"Waiting mock time of {mock_wait_time} seconds before checking S3 file")
        #     time.sleep(mock_wait_time)

        preview_data = load_json_from_s3(bucket_name, s3_key)
        logger.info(f"Successfully loaded preview data for UUID: {uuid}")

        # Optional cleanup logic (commented out for now)
        # try:
        #     s3_client.delete_object(Bucket=bucket_name, Key=s3_key)
        #     logger.info(f"Deleted preview file from S3: {s3_key}")
        # except Exception as cleanup_error:
        #     logger.warning(f"Failed to delete preview file: {cleanup_error}")

        return {
            "uuid": uuid,
            "previewData": preview_data
        }

    except FileNotFoundError:
        file_content = {"lambda_errors": [
            {
              f"reportLibrarylambda-amplify-einsteinRepLibMetaCard-DEV-ERR-008: Preview data file not found for UUID {uuid} in agentforcePreviewDataPull. Retry later. aws_request_id {jsonpickle.encode(str(context.aws_request_id))} at {dt.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f%Z')}"
            }
        ]}
        return file_content

    except Exception as e:
        logger.error(traceback.format_exc())
        file_content = {"lambda_errors": [
            {
              f"reportLibrarylambda-amplify-einsteinRepLibMetaCard-DEV-ERR-009: Unexpected error in agentforcePreviewDataPull for UUID {uuid}: {str(e)}. aws_request_id {jsonpickle.encode(str(context.aws_request_id))} at {dt.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f%Z')}"
            }
        ]}
        return file_content