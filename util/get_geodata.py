import os
import zipfile
import requests

# Data processing
import pandas as pd
import geopandas as gpd

DATA_FOLDER = 'gcs/data'

def download_land_cover(landcover_url, **kwargs):
    '''
    Download and extract Corine Land Cover 2018 geopackage
    and legend data from Sentinel Hub
    '''

    # Download main zip file from Sentinel Hub
    response = requests.get(landcover_url, stream=True)

    with open('corine_land_cover_2018.zip', 'wb') as main_zip:
        for chunck in response.iter_content(chunk_size=1024):
            if chunck:
                main_zip.write(chunck)

    # Unzip main zip file
    with zipfile.ZipFile('corine_land_cover_2018.zip', 'r') as zip_a:
        zip_a.extractall()

    # Extract geopackage and legend csv from second nested zip
    with zipfile.ZipFile('clc2018_clc2018_v2018_20_geoPackage.zip', 'r') as zip_b:

        filenames = zip_b.namelist()

        for filename in filenames:
            if filename.endswith(('.gpkg', 'legend.csv')):
                zip_b.extract(filename, DATA_FOLDER)


def download_aoi(aoi_geojson_url, **kwargs):
    '''
    Download area of interest provide by user
    '''

    response = requests.get(aoi_geojson_url)

    with open(os.path.join(DATA_FOLDER, 'aoi.geojson'), 'wb') as aoi:
        aoi.write(response.content)


def generate_labels_geodata(codes_to_keep, crs_output, **kwargs):
    '''
    Docstring tbd
    Description here
    '''

    geopackage_folder = os.path.join(DATA_FOLDER, 'clc2018_clc2018_v2018_20_geoPackage')
    geopackage_path = os.path.join(geopackage_folder, 'CLC2018_CLC2018_V2018_20.gpkg')

    # Load geodata and set coordinate reference system
    aoi = gpd.read_file(os.path.join(DATA_FOLDER, 'aoi.geojson'))
    aoi = aoi.to_crs({'init': crs_output})
    land_cover = gpd.read_file(geopackage_path, bbox=aoi)
    land_cover = land_cover.to_crs({'init': crs_output})

    # Load land cover legend (csv file)
    legend = pd.read_csv(os.path.join(geopackage_folder, 'Legend/CLC_legend.csv'), delimiter=';')
    land_cover['Code_18'] = land_cover['Code_18'].astype('int64')

    # Select codes of interest in legend
    legend_filtered = legend[
        legend['GRID_CODE'].isin(codes_to_keep)][['GRID_CODE', 'CLC_CODE', 'LABEL3']] \
        .reset_index(drop=True)

    # Export names and code numbers of selected labels into a separate file for future reference
    label_names = legend_filtered[['GRID_CODE', 'LABEL3']].copy().reset_index(drop=True)
    label_names.loc[len(label_names)] = [-1, 'Other land surface']
    label_names.to_csv(
        os.path.join(DATA_FOLDER, 'labels.csv'),
        index=True,
        index_label='LABEL_ID',
        encoding='utf-8')

    # Create a mapping of CLC CODES to a range between 0 and n
    # with n being the amount of selected classes
    all_clc_codes = legend.CLC_CODE.unique()
    selected_clc_codes = legend_filtered['CLC_CODE'].to_list()
    n_classes = len(selected_clc_codes) + 1

    selected_codes_map = {
        clc_code : index
        for index, clc_code in enumerate(selected_clc_codes)}

    other_codes_map = {
        clc_code : n_classes-1
        for clc_code in all_clc_codes
        if clc_code not in selected_clc_codes}

    # Add LABEL_ID column to land cover data using previous mapping
    land_cover['LABEL_ID'] = land_cover['Code_18'].map({**selected_codes_map, **other_codes_map})

    # Export final land cover data as GeoJson file
    land_cover.to_file(os.path.join(DATA_FOLDER, 'land_cover.geojson'), driver='GeoJSON')

    return n_classes
