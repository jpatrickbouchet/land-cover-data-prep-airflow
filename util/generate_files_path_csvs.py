import os
import random
import pandas as pd


def generate_fpath_csvs(gcs_bucket_dir, rand_seed, split, **context):
    """
    Main function for splitting the patches into train, validation and test datasets
    It outputs 3 csv files containing the patches paths
    """

    total_patches = context['ti'].xcom_pull(task_ids="get_satellite_images")
    train_idx, val_idx, test_idx = split_idx_randomly(total_patches, rand_seed, split)

    save_paths_as_csv(train_idx, gcs_bucket_dir, suffix='train')
    save_paths_as_csv(val_idx, gcs_bucket_dir, suffix='valid')
    save_paths_as_csv(test_idx, gcs_bucket_dir, suffix='test')

    return len(train_idx), len(val_idx), len(test_idx)


def split_idx_randomly(total_patches, rand_seed, split):
    """
    Helper function for splitting the patches ids randomly into 3 lists
    for training, validation and testing
    """

    random.seed(rand_seed)

    idx = list(range(total_patches))
    random.shuffle(idx)

    size_train = int(total_patches * split)
    size_val = int(total_patches * (1 - split) // 2)

    train_idx = idx[:size_train]
    val_idx = idx[size_train:size_train + size_val]
    test_idx = idx[size_train + size_val:]

    return train_idx, val_idx, test_idx


def save_paths_as_csv(idx_list, gcs_bucket_dir, suffix):
    """
    Helper function for saving patches paths into a csv file
    """

    rgb_data = []
    groundtruth_paths = []

    for idx in idx_list:
        eopatch_folder = os.path.join(gcs_bucket_dir, 'saved_patches', 'eopatch_{}'.format(idx))
        rgb_data.append(os.path.join(eopatch_folder, 'rgb_data.npy'))
        groundtruth_paths.append(os.path.join(eopatch_folder, 'groundtruth.npy'))

    paths = {
        'rgb_data': rgb_data,
        'groundtruth': groundtruth_paths
    }

    csv_path = os.path.join(gcs_bucket_dir, '{0}_fpaths.csv'.format(suffix))

    pd.DataFrame(paths).to_csv(csv_path, sep=",", header=False, index=False, encoding='utf-8')
