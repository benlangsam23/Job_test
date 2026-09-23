"""
CMS Hospital Dataset Downloader
--------------------------------
Downloads all 75 "Hospitals" theme CSVs from data.cms.gov.
Parallel downloads (ThreadPoolExecutor)
Incremental: skips files whose 'modified' date hasn't changed since last run
Converts all CSV headers to snake_case
Persists download state in _tracker.json
Usage:
python download_cms_hospitals.py # default output dir
python download_cms_hospitals.py /path/to/output # custom output dir
Schedule daily via cron:
0 6 * * * /usr/bin/python3 /path/to/download_cms_hospitals.py /data/cms >> /var/log/cms_download.log 2>&1
"""
import json
import os
import re
import csv
import io
import sys
import subprocess
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
DEFAULT_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cms_hospital_data")
MAX_WORKERS = 8
CMS_CATALOG = [
{"identifier":"48nr-hqxx","title":"OAS CAHPS - ASC Facility","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/8392b7d74209bb3dc54ff1b09635e733_1785189943/ASCQR_OAS_CAHPS_BY_ASC.csv"},
{"identifier":"4jcv-atw7","title":"ASC Quality Measures - Facility","modified":"2025-12-16","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/dd03994fc93e296bb0297f1cd43cc987_1770163552/ASC_Facility.csv"},
{"identifier":"wue8-3vwe","title":"ASC Quality Measures - National","modified":"2025-12-16","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/43dcc3212bfab34ac186ac2e19196b09_1770163554/ASC_National.csv"},
{"identifier":"tf3h-mrrs","title":"OAS CAHPS - ASC National","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/65a38069cc48376e9f519a3609424537_1785189945/ASCQR_OAS_CAHPS_NATIONAL.csv"},
{"identifier":"axe7-s95e","title":"ASC Quality Measures - State","modified":"2025-12-16","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/d3aea2dfaeae85c44017441b184aeb6d_1770163558/ASC_State.csv"},
{"identifier":"x663-bwbj","title":"OAS CAHPS - ASC State","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/f086b4b7fc9d628f08e70ade47dff3c5_1785189946/ASCQR_OAS_CAHPS_STATE.csv"},
{"identifier":"tqkv-mgxq","title":"Comprehensive Care Joint Replacement - Provider","modified":"2026-01-26","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/c1017e56164f4774560619c9020116fa_1770163562/CJR_Quality_Reporting_January_2026_Production_File.csv"},
{"identifier":"muwa-iene","title":"CMS Medicare PSI-90 Six-digit","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/84bd78c1b4e386185bcef2af963d8cf9_1785189947/CMS_PSI_6_decimal_file.csv"},
{"identifier":"ynj2-r877","title":"Complications and Deaths - Hospital","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/6af7c44d77436e5a1caac3ce39a83fe9_1785189947/Complications_and_Deaths-Hospital.csv"},
{"identifier":"qqw3-t4ie","title":"Complications and Deaths - National","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/aba37eca11edcd4272afff9e6dc78bda_1785189947/Complications_and_Deaths-National.csv"},
{"identifier":"bs2r-24vh","title":"Complications and Deaths - State","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/247bb4364e310313773d74ccb0fb3231_1785189948/Complications_and_Deaths-State.csv"},
{"identifier":"y9us-9xdf","title":"Footnote Crosswalk","modified":"2025-09-18","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/f29bb7c812e242f6edfef0a4b7d0eaca_1760630713/Footnote_Crosswalk.csv"},
{"identifier":"xrgf-x36b","title":"FY2024 Distribution Net Change Base Op DRG","modified":"2025-12-09","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/c9c4b48e76ed7ca1e965bd39959d497b_1770163573/FY2024_Distribution_of_Net_Change_in_Base_Op_DRG_Payment_Amt.csv"},
{"identifier":"5gv4-jwyv","title":"FY2024 Net Change Base Op DRG","modified":"2025-12-09","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/1b4d8a2b70cd651403cf1c4f8e545088_1770163575/FY2024_Net_Change_in_Base_Op_DRG_Payment_Amt.csv"},
{"identifier":"u625-zae7","title":"FY2024 Percent Change Medicare Payments","modified":"2025-12-09","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/3d267e89dc34ca3a928554b9c73775ef_1770163577/FY2024_Percent_Change_in_Medicare_Payments.csv"},
{"identifier":"vtqa-m4zn","title":"FY2024 Value-Based Incentive Payment","modified":"2025-12-09","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/ba15e25a73a051dbb3435458af7eb96d_1770163578/FY2024_Value_Based_Incentive_Payment_Amount.csv"},
{"identifier":"dgck-syfz","title":"HCAHPS - Hospital","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/78a50346fbe828ea0ce2837847af6a7c_1785189950/HCAHPS-Hospital.csv"},
{"identifier":"99ue-w85f","title":"HCAHPS - National","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/08eefc5ab7c6cce7b3555fd79b0d8eec_1785189951/HCAHPS-National.csv"},
{"identifier":"84jm-wiui","title":"HCAHPS - State","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/e4ca53701c73500a3188874f2bbd2854_1785189951/HCAHPS-State.csv"},
{"identifier":"77hc-ibv8","title":"HAI - Hospital","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/43825e12dc0c923df9ba5cbdf473c9d5_1785189952/Healthcare_Associated_Infections-Hospital.csv"},
{"identifier":"yd3s-jyhd","title":"HAI - National","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/a87585b27c50c3899eec6a857958a5f0_1785189953/Healthcare_Associated_Infections-National.csv"},
{"identifier":"k2ze-bqvw","title":"HAI - State","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/27986ce2f1193d3fcad7fa1bddc93c3b_1785189953/Healthcare_Associated_Infections-State.csv"},
{"identifier":"yizn-abxn","title":"OAS CAHPS - HOPD Facility","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/9189f27bb8ab7a4ff8919bdc682bf79a_1785189954/OQR_OAS_CAHPS_BY_HOSPITAL.csv"},
{"identifier":"s5pj-hua3","title":"OAS CAHPS - HOPD National","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/fdc286f2f60134775fa4b7f7b249b075_1785189954/OQR_OAS_CAHPS_NATIONAL.csv"},
{"identifier":"6pfg-whmx","title":"OAS CAHPS - HOPD State","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/5c995348b26c08179f1149d3b07dcb70_1785189954/OQR_OAS_CAHPS_STATE.csv"},
{"identifier":"xubh-q36u","title":"Hospital General Information","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/893c372430d9d71a1c52737d01239d47_1785189955/Hospital_General_Information.csv"},
{"identifier":"z8ax-x9j1","title":"Complications - PCH Hospital","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/bc386a698d7a9b8d09bf37a73371a899_1785189956/PCH_Complications_Unplanned_Hospital_Visits_HOSPITAL.csv"},
{"identifier":"jfnd-nl7s","title":"Complications - PCH National","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/6a91b2a437ea47e61cd34c975dd54729_1785189956/PCH_Complications_Unplanned_Hospital_Visits_NATIONAL.csv"},
{"identifier":"yq43-i98g","title":"HAC Reduction Program","modified":"2026-01-26","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/74be67fd6833391f578abb5605d03ce6_1770163605/FY_2026_HAC_Reduction_Program_Hospital.csv"},
{"identifier":"5hk7-b79m","title":"MSPB - Additional Decimals","modified":"2026-01-26","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/500f70bcb6c65433c00a96af0e0c0430_1770163607/HOSPITAL_QUARTERLY_MSPB_6_DECIMALS.csv"},
{"identifier":"k653-4ka8","title":"Safety HAI - PCH","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/d24fd9fa17ad5673de9cf6220c1c7a5a_1785189957/PCH_HEALTHCARE_ASSOCIATED_INFECTIONS_HOSPITAL.csv"},
{"identifier":"iy27-wz37","title":"PCH HCAHPS - Hospital","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/155aff6db1e04cbbe1676ce459d71053_1785189958/PCH_HCAHPS_HOSPITAL.csv"},
{"identifier":"9g7e-btyt","title":"PCH HCAHPS - National","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/51dabafd670b6379256fc111bcbec8b4_1785189958/PCH_HCAHPS_NATIONAL.csv"},
{"identifier":"qatj-nmws","title":"PCH HCAHPS - State","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/f1d65b1e3b562b932fd3827c7783ea35_1785189959/PCH_HCAHPS_STATE.csv"},
{"identifier":"9n3s-kdb3","title":"Hospital Readmissions Reduction Program","modified":"2026-01-26","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/a171bc36c488d3e0dc33ec63abb469a6_1770163617/FY_2026_Hospital_Readmissions_Reduction_Program_Hospital.csv"},
{"identifier":"pudb-wetr","title":"HVBP - Clinical Outcomes","modified":"2026-01-26","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/f5c74a8e8a0f2017f6e13b6fc517ae4e_1770163619/hvbp_clinical_outcomes.csv"},
{"identifier":"su9h-3pvj","title":"HVBP - Efficiency Scores","modified":"2026-01-26","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/46325dbbe6d2f654ecbb5046059c664e_1770163621/hvbp_efficiency_and_cost_reduction.csv"},
{"identifier":"avtz-f2ge","title":"HVBP - Person Community Engagement","modified":"2026-01-26","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/ccc6534894a9bfe22b5f986260ed359e_1770163624/hvbp_person_and_community_engagement.csv"},
{"identifier":"dgmq-aat3","title":"HVBP - Safety","modified":"2026-04-28","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/33e26123c259ca779642b0d61b2e82f5_1777392336/hvbp_safety.csv"},
{"identifier":"ypbt-wvdk","title":"HVBP - Total Performance Score","modified":"2026-01-26","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/5551d4839c1dd75e3f7fe1310a1e2369_1770163628/hvbp_tps.csv"},
{"identifier":"q9vs-r7wp","title":"IPFQR - Facility","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/e56cdbea2d002358610ebdb957cb35de_1785189962/IPFQR_QualityMeasures_Facility.csv"},
{"identifier":"s5xg-sys6","title":"IPFQR - National","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/463e16600bec8c0aeb75ebfaef253ab7_1785189962/IPFQR_QualityMeasures_National.csv"},
{"identifier":"dc76-gh7x","title":"IPFQR - State","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/aaf5b6168e889e217580d86b5c443c6d_1785189963/IPFQR_QualityMeasures_State.csv"},
{"identifier":"4j6d-yzce","title":"Measure Dates","modified":"2026-07-28","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/633113381312e1fa86c8e0ba8b14ed0f_1785189963/Measure_Dates.csv"},
{"identifier":"nrth-mfg3","title":"Medicare Hospital Spending by Claim","modified":"2026-01-26","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/1f8cde9e222d5d49f88a894bcf7a8981_1770163638/Medicare_Hospital_Spending_by_Claim.csv"},
{"identifier":"rrqw-56er","title":"MSPB - Hospital","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/69874ce604586980ac088283c1b35095_1785189964/Medicare_Hospital_Spending_Per_Patient-Hospital.csv"},
{"identifier":"3n5g-6b7f","title":"MSPB - National","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/ff15a8451b83ebc70b85a5c0e870a751_1785189965/Medicare_Hospital_Spending_Per_Patient-National.csv"},
{"identifier":"rs6n-9qwg","title":"MSPB - State","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/b53c8e7d24557eb1e898557e4c7d7484_1785189965/Medicare_Hospital_Spending_Per_Patient-State.csv"},
{"identifier":"wkfw-kthe","title":"Outpatient Imaging Efficiency - Hospital","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/d0134a9a89e59d49426cf5a2ad063b60_1785189966/Outpatient_Imaging_Efficiency-Hospital.csv"},
{"identifier":"di9i-zzrc","title":"Outpatient Imaging Efficiency - National","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/78172a595e76dc064ea87884c5aea1f8_1785189966/Outpatient_Imaging_Efficiency-National.csv"},
{"identifier":"if5v-4x48","title":"Outpatient Imaging Efficiency - State","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/6fb7a772c39e34e630e7e97ec4887263_1785189967/Outpatient_Imaging_Efficiency-State.csv"},
{"identifier":"yv7e-xc69","title":"Timely and Effective Care - Hospital","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/0437b5494ac61507ad90f2af6b8085a7_1785189967/Timely_and_Effective_Care-Hospital.csv"},
{"identifier":"isrn-hqyy","title":"Timely and Effective Care - National","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/b298b4a8c05addc75d1e9c9c9b2e4706_1785189968/Timely_and_Effective_Care-National.csv"},
{"identifier":"apyc-v239","title":"Timely and Effective Care - State","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/c4f74a440cc6ce4ed941fa3c9de2ab58_1785189968/Timely_and_Effective_Care-State.csv"},
{"identifier":"632h-zaca","title":"Unplanned Hospital Visits - Hospital","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/30edc1d0417a34b58affcc2495a02b0a_1785189969/Unplanned_Hospital_Visits-Hospital.csv"},
{"identifier":"cvcs-xecj","title":"Unplanned Hospital Visits - National","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/d30b0557f1d06bee1d5646d2eaede709_1785189969/Unplanned_Hospital_Visits-National.csv"},
{"identifier":"4gkm-5ypv","title":"Unplanned Hospital Visits - State","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/73ce05bc6ecbb5def0e859818a906f7b_1785189970/Unplanned_Hospital_Visits-State.csv"},
{"identifier":"6qxe-iqz8","title":"VHA Behavioral Health","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/0742f326b7a19b57bc01136b2cd8b7b6_1785189970/VA_IPF.csv"},
{"identifier":"ptds-r8im","title":"VHA Timely and Effective Care","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/07084088a9de9e44c51bef291a4e163a_1785189971/VA_TE.csv"},
{"identifier":"uyx4-5s7f","title":"VHA Provider Level Data","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/9cea8696850b78e02559183bd6071fd6_1785189971/Veterans_Health_Administration_Provider_Level_Data.csv"},
{"identifier":"bzsr-4my4","title":"Data Updates","modified":"2026-07-15","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/f751bebc45c1ee84120ea833308faef2_1785189971/Data_Updates_July_2026.csv"},
{"identifier":"nrdb-3fcy","title":"Maternal Health - Hospital","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/5a4754b088fdb10d2ae278ef215925a7_1785189972/Maternal_Health-Hospital.csv"},
{"identifier":"mxtu-43qs","title":"Patient-Reported Outcomes - Hospital","modified":"2026-08-05","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/60ee1962356ca17e50e12f95d2871b46_1785189972/PATIENT_REPORTED_OUTCOMES_FACILITY.csv"},
{"identifier":"qoeg-w7ck","title":"Palliative Care - PCH Hospital","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/2d420234bc6bcc963c6c374d91b0ed90_1785189973/PCH_Palliative_Care_HOSPITAL.csv"},
{"identifier":"qigt-w5cx","title":"Palliative Care - PCH National","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/b19666c7d9f040b0fffd9c7135a7d92d_1785189973/PCH_Palliative_Care_NATIONAL.csv"},
{"identifier":"f4ga-b9gx","title":"Promoting Interoperability - Hospital","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/5462b19a756c53c1becccf13787d9157_1785189974/Promoting_Interoperability-Hospital.csv"},
{"identifier":"hbf-map","title":"Birthing Friendly Hospitals Geocoded","modified":"2026-07-28","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/e7f75e0803a17e22c4e26acf2183e622_1786493184/Birthing_Friendly_Hospitals_Geocoded.csv"},
{"identifier":"97xg-v3wv","title":"REH Timely Effective Care - Hospital","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/325633e3e81e17de313537518fdca092_1785189974/REH_Timely_and_Effective_Care-Hospital.csv"},
{"identifier":"d2k3-k3ac","title":"REH Timely Effective Care - National","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/3e7a7fe925204d9f476690ad30ffcb28_1785189975/REH_Timely_and_Effective_Care-National.csv"},
{"identifier":"7peb-i4pi","title":"REH Outpatient Imaging - Hospital","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/1a7240761452e8430813299b2e5b28e1_1785189976/REH_Outpatient_Imaging_Efficiency-Hospital.csv"},
{"identifier":"zt3q-3e1z","title":"REH Outpatient Imaging - National","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/400efdf48b2a73bfef6217d28bf082e3_1785189978/REH_Outpatient_Imaging_Efficiency-National.csv"},
{"identifier":"zez1-ka2w","title":"REH Unplanned Visits - Hospital","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/8058b83869a5ba155b84c3227e60f61c_1785189978/REH_Unplanned_Hospital_Visits-Hospital.csv"},
{"identifier":"uk3n-au7a","title":"REH Unplanned Visits - National","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/ab50380fe5f8578c9b07e118dcdc43e4_1785189979/REH_Unplanned_Hospital_Visits-National.csv"},
{"identifier":"1su6-zfft","title":"Patient Engagement - PCH Hospital","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/1ee80985ad5e9a217dd0208b0e0d4b78_1785189979/PCH_Patient_Engagement_Hospital.csv"},
{"identifier":"4dyz-5kt1","title":"Patient Engagement - PCH National","modified":"2026-07-22","downloadURL":"https://data.cms.gov/provider-data/sites/default/files/resources/887d52f819864261d84b0de6ac71a65c_1785189979/PCH_Patient_Engagement_National.csv"},
]

def to_snake_case(header):
s = header.strip()
s = re.sub(r'[^a-zA-Z0-9]+', '_', s)
s = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s)
s = re.sub(r'_+', '_', s).strip('_').lower()
return s

def load_tracker(tracker_file):
if os.path.exists(tracker_file):
with open(tracker_file, 'r') as f:
return json.load(f)
return {}

def save_tracker(tracker, tracker_file):
with open(tracker_file, 'w') as f:
json.dump(tracker, f, indent=2)

def fetch_csv(url):
result = subprocess.run(
["curl", "-sS", "-L", "--noproxy", "", "--max-time", "120",
"-H", "Accept: text/csv,/", url],
capture_output=True
)
if result.returncode != 0:
raise RuntimeError(f"curl failed: {result.stderr.decode()}")
return result.stdout.decode("utf-8-sig")

def download_and_process(dataset, tracker, data_dir):
ds_id = dataset["identifier"]
title = dataset["title"]
modified = dataset["modified"]
url = dataset["downloadURL"]
prev_modified = tracker.get(ds_id, {}).get("modified")
if prev_modified == modified:
return ds_id, "skipped", f"{title} - not modified since {modified}"
try:
raw = fetch_csv(url)
except Exception as e:
return ds_id, "error", f"{title} - {e}"
reader = csv.reader(io.StringIO(raw))
try:
original_headers = next(reader)
except StopIteration:
return ds_id, "error", f"{title} - empty CSV"
snake_headers = [to_snake_case(h) for h in original_headers]
rows = list(reader)
safe_name = re.sub(r'[^a-zA-Z0-9_-]', '_', ds_id)
out_path = os.path.join(data_dir, f"{safe_name}.csv")
with open(out_path, 'w', newline='', encoding='utf-8') as f:
writer = csv.writer(f)
writer.writerow(snake_headers)
writer.writerows(rows)
return ds_id, "downloaded", f"{title} - {len(rows)} rows -> {out_path}"

def main():
data_dir = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_DATA_DIR
tracker_file = os.path.join(data_dir, "_tracker.json")
os.makedirs(data_dir, exist_ok=True)
tracker = load_tracker(tracker_file)
run_time = datetime.now(timezone.utc).isoformat()
print(f"[{run_time}] Starting CMS Hospital data download")
print(f" Datasets in catalog: {len(CMS_CATALOG)}")
print(f" Output directory: {data_dir}")
print(f" Previously tracked: {len(tracker)} datasets")
print()
results = {"downloaded": 0, "skipped": 0, "error": 0}
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
futures = {
pool.submit(download_and_process, ds, tracker, data_dir): ds
for ds in CMS_CATALOG
}
for future in as_completed(futures):
ds_id, status, msg = future.result()
results[status] += 1
icon = {"downloaded": "+", "skipped": "=", "error": "!"}[status]
print(f" [{icon}] {msg}")
if status == "downloaded":
ds = futures[future]
tracker[ds_id] = {
"modified": ds["modified"],
"last_downloaded": run_time,
}
save_tracker(tracker, tracker_file)
print()
print(f"Done: {results['downloaded']} downloaded, "
f"{results['skipped']} skipped, {results['error']} errors")

if __name__ == "__main__":
main()
