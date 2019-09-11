from google.cloud import storage
from io import BytesIO

# Data processing
import numpy as np
import geopandas as gpd

# Imports from sentinelhub-py and eo-learn
from sentinelhub import BBoxSplitter
from eolearn.io import S2L2AWCSInput
from eolearn.core import (EOTask, LinearWorkflow, FeatureType, EOExecutor)

from eolearn.geometry import VectorToRaster


def get_satellite_images(resolution, crs_output, time_interval, img_size, output_bucket, **kwargs):
    """
    Main function for generating satellite images (patches) using the EO-Learn API
    """

    path_land_cover = 'gcs/data/land_cover.geojson'
    path_aoi = 'gcs/data/aoi.geojson'

    # Get aoi dimensions in meters
    aoi_shape, aoi_width, aoi_height = get_aoi_dimensions(path_aoi, crs_output)

    # Load labels geodata
    labels_data = gpd.read_file(path_land_cover)
    labels_data = labels_data.to_crs(crs={'init': crs_output})

    # Compute number of columns and rows based on expected image dimensions output
    columns = int((aoi_width / img_size) / resolution)
    rows = int((aoi_height / img_size) / resolution)

    # Create splitter to obtain list of bounding boxes
    bbox_splitter = BBoxSplitter([aoi_shape], crs_output, (columns, rows))

    # Create and execute workflow
    workflow, input_task, save_task = create_workflow(resolution, labels_data, output_bucket)

    total_patches_created = execute_workflow(workflow,
                                             input_task,
                                             save_task,
                                             bbox_splitter,
                                             time_interval)

    return total_patches_created


def get_aoi_dimensions(path_aoi, crs_output):
    """
    Helper function to compute the dimension of an area of interest
    for a specific Coordinate Reference System
    """

    aoi = gpd.read_file(path_aoi)

    aoi = aoi.to_crs(crs={'init': crs_output})

    aoi_shape = aoi.geometry.values.tolist()[-1]
    aoi_width = aoi_shape.bounds[2] - aoi_shape.bounds[0]
    aoi_height = aoi_shape.bounds[3] - aoi_shape.bounds[1]

    return aoi_shape, aoi_width, aoi_height


class SaveToGcp(EOTask):
    """
    EO-Learn custom task for saving each image and groundtruth to output GCS bucket
    """
    def __init__(self, feature_rgb, feature_mask, output_bucket):
        self.feature_rgb_type, self.feature_rgb_name = next(self._parse_features(feature_rgb)())
        self.feature_mask_type, self.feature_mask_name = next(self._parse_features(feature_mask)())
        self.output_bucket = output_bucket

    def execute(self, eopatch, eopatch_folder):

        data = {
            'rgb_data': eopatch[self.feature_rgb_type][self.feature_rgb_name],
            'groundtruth': eopatch[self.feature_mask_type][self.feature_mask_name]
        }

        client = storage.Client()
        bucket = client.get_bucket(self.output_bucket)

        for item in data:
            byte_io = BytesIO()
            np.save(byte_io, data[item])
            blob = bucket.blob('saved_patches/{}/{}.npy'.format(eopatch_folder, item))
            blob.upload_from_string(byte_io.getvalue())

        return eopatch


class MedianPixel(EOTask):
    """
    EO-Learn custom task to return a pixelwise median value from a time-series
    and stores the results in a timeless data array.
    """
    def __init__(self, feature, feature_out):
        self.feature_type, self.feature_name = next(self._parse_features(feature)())
        self.feature_type_out, self.feature_name_out = next(self._parse_features(feature_out)())

    def execute(self, eopatch):
        eopatch.add_feature(self.feature_type_out, self.feature_name_out,
                            np.median(eopatch[self.feature_type][self.feature_name], axis=0))
        return eopatch


def create_workflow(resolution, land_cover_data, output_bucket):
    """
    Helper function for creating the EO-Learn workflow
    """

    # Maximum allowed cloud cover of original ESA tiles
    maxcc = 0.2

    # Task to get S2 L2A images
    input_task = S2L2AWCSInput(
        layer='TRUE_COLOR',
        resx='{}m'.format(resolution), # resolution x
        resy='{}m'.format(resolution), # resolution y
        maxcc=maxcc, # maximum allowed cloud cover of original ESA tiles
    )

    # Task to rasterize ground-truth from Corine Land Cover 2018
    rasterization_task = VectorToRaster(land_cover_data,
                                        (FeatureType.MASK_TIMELESS, 'LAND_COVER'),
                                        values_column='LABEL_ID',
                                        raster_shape=(FeatureType.MASK, 'IS_DATA'),
                                        raster_dtype=np.uint8)

    # Task to compute pixelwise median values pixelwise our time-series
    get_median_pixel_task = MedianPixel((FeatureType.DATA, 'TRUE_COLOR'),
                                        feature_out=(FeatureType.DATA_TIMELESS, 'MEDIAN_PIXEL'))

    save_task = SaveToGcp((FeatureType.DATA_TIMELESS, 'MEDIAN_PIXEL'),
                          (FeatureType.MASK_TIMELESS, 'LAND_COVER'),
                          output_bucket)

    # Putting workflow together
    workflow = LinearWorkflow(input_task, rasterization_task, get_median_pixel_task, save_task)

    return workflow, input_task, save_task


def execute_workflow(workflow, input_task, save_task, bbox_splitter, time_interval):
    """
    Helper function for executing the EO-Learn workflow
    """

    bbox_list = np.array(bbox_splitter.get_bbox_list())
    total_patches = len(bbox_splitter.bbox_list)

    # Define additional parameters of the workflow
    execution_args = [
        {
            input_task: {'bbox':bbox_list[idx], 'time_interval':time_interval},
            save_task: {'eopatch_folder': 'eopatch_{}'.format(idx)}
        } for idx in range(total_patches)
    ]

    executor = EOExecutor(workflow, execution_args, save_logs=True)
    executor.run(workers=5, multiprocess=True)

    return total_patches
