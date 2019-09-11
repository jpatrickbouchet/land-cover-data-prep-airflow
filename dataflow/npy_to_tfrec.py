from __future__ import print_function

from io import BytesIO

import argparse
import os

import subprocess
import numpy as np
import tensorflow as tf
import apache_beam as beam
from apache_beam.options.pipeline_options import SetupOptions


def _int64_feature(value):
    """Wrapper for inserting int64 features into Example proto."""
    if not isinstance(value, list):
        value = [value]
    return tf.train.Feature(int64_list=tf.train.Int64List(value=value))


def _bytes_feature(value):
    """Wrapper for inserting bytes features into Example proto."""
    return tf.train.Feature(bytes_list=tf.train.BytesList(value=[value]))


def _convert_to_example(image_buffer, mask_buffer):
    """Build an Example proto for an example."""

    example = tf.train.Example(
        features=tf.train.Features(
            feature={
                'img_encoded': _bytes_feature(image_buffer),
                'mask_encoded': _bytes_feature(mask_buffer)
            }))
    return example


def load_npy_file(image_data, channels):

    def np_load(image_data):
        return np.load(BytesIO(image_data))

    if channels == 1:
        dtype = tf.uint8
    else:
        dtype = tf.float32

    with tf.Session() as sess:
        image = sess.run(tf.py_func(np_load, [image_data], dtype))

    return image


def _get_image_data(filepath, channels):
    """Process a single image file."""
    # Read npy file
    with tf.gfile.GFile(filepath, 'rb') as ifp:
        image_data = ifp.read()

    # Load file
    image = load_npy_file(image_data, channels)

    # Rescale rgb image array from 0-1 to 0-255 range
    if channels == 3:
        image = image*255
        image = image.astype('uint8')

    # Check that image converted properly and get dimensions
    assert len(image.shape) == 3
    height = image.shape[0]
    width = image.shape[1]
    assert image.shape[2] == channels

    # Encode image to png
    with tf.Session() as sess:
        image_enc = sess.run(tf.image.encode_png(image))

    return image_enc, height, width


def convert_to_example(csvline):
    """Parse a line of CSV file and convert to TF Record."""

    path_img, path_mask = csvline.encode('ascii', 'ignore').split(',')

    image_buffer, height, width = _get_image_data(path_img, channels=3)
    mask_buffer, height_m, width_m = _get_image_data(path_mask, channels=1)

    assert height == height_m
    assert width == width_m

    example = _convert_to_example(image_buffer, mask_buffer)

    yield example.SerializeToString()


def run(argv=None):
    """Main entry point; defines and runs the pipeline."""
    
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--train_csv',
        # pylint: disable=line-too-long
        dest='train_csv',
        help=
        'Contains paths of generated patches (.npy). Each line has 2 paths separated by a comma: 1 for rgb image, 1 for groundtruth',
        required=True)
    parser.add_argument(
        '--validation_csv',
        # pylint: disable=line-too-long
        dest='validation_csv',
        help=
        'Contains paths of generated patches (.npy). Each line has 2 paths separated by a comma: 1 for rgb image, 1 for groundtruth',
        required=True)
    parser.add_argument(
        '--output_dir',
        dest='output_dir',
        help='Top-level directory for TF Records',
        required=True)

    known_args, pipeline_args = parser.parse_known_args(argv)
    arguments = known_args.__dict__

    output_dir = arguments['output_dir']

    try:
        subprocess.check_call('gsutil -m rm -r {}'.format(output_dir).split())
    except subprocess.CalledProcessError:
        pass

    # pylint: disable=line-too-long
    opts = beam.pipeline.PipelineOptions(pipeline_args)
    opts.view_as(SetupOptions).save_main_session = True
    with beam.Pipeline(options=opts) as p:
        # BEAM tasks
        for step in ['train', 'validation']:
            _ = (
                p
                | '{}_read_csv'.format(step) >> beam.io.ReadFromText(arguments['{}_csv'.format(step)])
                | '{}_convert'.format(step) >> beam.FlatMap(lambda line: convert_to_example(line))
                | '{}_write_tfr'.format(step) >> beam.io.tfrecordio.WriteToTFRecord(os.path.join(output_dir, step)))

if __name__ == '__main__':
    run()
