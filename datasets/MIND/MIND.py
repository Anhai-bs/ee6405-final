import os as _os_
import datasets as _datasets_
import pandas as _pandas_
import pyarrow as _pyarrow_


class MINDataset(_datasets_.ArrowBasedBuilder):
    r"""
    A custom dataset class for loading the MIND dataset.

    TODO citation
    """

    BUILDER_CONFIGS = [
        _datasets_.BuilderConfig(
            name='news', 
        ),
        _datasets_.BuilderConfig(
            name='behaviors', 
        ),
    ]

    def _info(self):
        return _datasets_.DatasetInfo()

    def _split_generators(self, dl_manager):
        base_url = 'https://mind201910small.blob.core.windows.net/release'
        unarchived = lambda name: dl_manager.download_and_extract(f'{base_url}/{name}')
        # BUILDER_CONFIGS config name and corresponding data file paths
        dataset_name = {
            'news': 'news.tsv',
            'behaviors': 'behaviors.tsv',
        }[self.config.name]
        return [
            _datasets_.SplitGenerator(
                name=split_name, 
                gen_kwargs=dict(
                    filepath=_os_.path.join(unarchived(archive_name), dataset_name),
                ),
            ) for split_name, archive_name, dataset_name in [
                (_datasets_.Split.TEST, 'MINDlarge_test.zip', dataset_name),
                (_datasets_.Split.TRAIN, 'MINDlarge_train.zip', dataset_name),
                (_datasets_.Split.VALIDATION, 'MINDlarge_dev.zip', dataset_name),
            ]
        ]

    def _generate_tables(self, filepath, **kwargs):
        dataset_colnames = {
            'news': [
                'news_id',
                'category',
                'subcategory',
                'title',
                'abstract',
                'url',
                'title_entities',
                'abstract_entities',
            ],
            'behaviors': [
                'impression_id',
                'user_id',
                'time',
                'history_news_ids',
                'impression_news_ids',
            ],
        }[self.config.name]
        df = _pandas_.read_csv(
            filepath, 
            delimiter='\t', 
            header=None, 
            encoding='utf-8',
            names=dataset_colnames,
        )
        yield str(filepath), _pyarrow_.Table.from_pandas(df)
