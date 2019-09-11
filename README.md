# landcover-data-prep-airflow

Airflow dag for generating training data for a land segmentation ML model using the EO-Learn API and Geopandas.
It is meant to be run on Google Cloud Composer and output to a GCS bucket.

NOTE:  
Although it worked well on my side, it might not be bullet proof for all scenarios.
It was mostly for my own practice.  

**1. Get url of Corine Land Cover 2018 geopackage (zip file)**  
https://land.copernicus.eu/pan-european/corine-land-cover/clc2018?tab=download
\
\
**2. Create a Sentinel Hub account and take note of your instance ID**
You will find your instance ID in the Configuration Utility section in the "Simple WMS template" area
\
\
**3. Create a new layer called TRUE-COLOR-S2-L2A on Sentinel Hub**
````
 1. Click "Edit" in Configuration Utility section (in Simple WMS template area)
 2. Then "Add new layer" at the top
			 . Enter "TRUE-COLOR-S2-L2A" as name
			 . In "Data Processing" select "TRUE_COLOR" as Base Product and "RGB visualization" in Visualization options
			 . Copy script to editor
			 . Click on "Set Custom Script"
 3. Save layer
 ````
\
\
**4. Create your area of interest as Geojson (Europe only)**  
There should only be 1 geometry in the Geojson
\
\
\
**5. Create and set a gcloud project** 
\
\
\
**6. Create a Cloud Composer environment**
```
gcloud beta composer environments create landcover-data-prep \
--location us-central1 \
--zone us-central1-b \
--machine-type c2-standard-4 \
--disk-size 100 \
--image-version composer-latest-airflow-1.10.2 \
--labels env=beta \
--python-version=3
```
\
**7. Install required packages in environment**
```
gcloud composer environments update landcover-data-prep \
--update-pypi-packages-from-file requirements.txt \
--location us-central1
```
\
**8. Fill in and upload variables_config.json file**
```
{
	"variables_config": {
		"email": ["your_email_here"],
		"landcover_url": "corine_land_cover_2018_url_here",
		"aoi_geojson_url": "url_here",
		"sentinelhub_instance_id": "instance_id_here",
		"crs_output": "epsg:32631",
		"project_id": "your_google_cloud_project_id",
		"bucket_output": "your_google_cloud_bucket",
		"region": "europe-west4",
		"zone": "europe-west4-a"
	}
}
```
\
**9. Upload dag and assets to GCP with:**
```
./upload_to_gcp.sh
```
\
**10. Trigger dag manually**
