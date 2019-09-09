# -*- coding: utf-8 -*-

import os
import pandas as pd
import numpy as np
from ddf_utils.str import to_concept_id

# path
source_path = '../source/IHME_GLOBAL_EDUCATIONAL_ATTAINMENT_1970_2015/'
out_path = '../../'
cb_csv = os.path.join(
    source_path,
    'IHME_GLOBAL_EDUCATIONAL_ATTAINMENT_1970_2015_CB_Y2015M04D27.CSV')
data_csv = os.path.join(
    source_path,
    'IHME_GLOBAL_EDUCATIONAL_ATTAINMENT_1970_2015_Y2015M04D27.CSV')

if __name__ == '__main__':
    print('running etl for ddf--ihme--edu_attainment...')
    # read codebook csv
    cb = pd.read_csv(cb_csv, skiprows=1)
    cb = cb.drop('Variable:', axis=1)  # unneeded column
    # read data csv
    data = pd.read_csv(data_csv)

    # now begins the etl process
    names = list()
    measures = list()

    data['metric'] = data['metric'].map(to_concept_id)

    # each metric will act as a measure, and there are 3 parts of data
    # for each measure
    for met in cb.metric.drop(0).dropna().unique():
        met_id = to_concept_id(met)
        df = data.groupby(by='metric').get_group(met_id)
        for i in ['mean', 'lower', 'upper']:
            # append the measure and measure name lists.
            measure = '{}_{}'.format(met_id, i)
            measures.append(measure)
            if i in ['lower', 'upper']:
                name = '{}, 95% Uncertainty Interval - {} Bound'.format(
                    met, i.title())
            else:
                name = '{}: Mean'.format(met)
            names.append(name)

            # save datapoints
            df = df.rename(columns={i: measure})
            df_out = df[['location_code', 'age_group_name', 'sex_name', 'year', measure]].copy()
            df_out.columns = ['location', 'age_group', 'sex', 'year', measure]
            df_out[
                ['location', 'age_group', 'sex']] = (df_out[['location', 'age_group', 'sex']]
                                                     .applymap(to_concept_id))
            df_out = df_out.drop_duplicates()
            df_out.to_csv(
                os.path.join(
                    out_path,
                    'ddf--datapoints--{}--by--location--age_group--sex--year.csv'.
                    format(measure)),
                index=False,
                float_format='%.2f')

    # entities
    loc = data[['location_id', 'location_code',
                'location_name']].drop_duplicates()
    # we use location code for entity key, so we want to assure there are no duplicates
    assert not np.all(loc['location_name'].duplicated())
    loc['location'] = loc['location_code'].map(to_concept_id)
    loc.set_index('location').to_csv(
        os.path.join(out_path, 'ddf--entities--location.csv'))

    sex = data[['sex_id', 'sex_name']].drop_duplicates()
    sex['sex'] = sex['sex_name'].map(to_concept_id)
    sex.set_index('sex').to_csv(
        os.path.join(out_path, 'ddf--entities--sex.csv'))

    age = data[['age_group_id', 'age_group_name']].drop_duplicates()
    age['age_group'] = age['age_group_name'].map(to_concept_id)
    age.set_index('age_group').to_csv(
        os.path.join(out_path, 'ddf--entities--age_group.csv'))

    # concepts
    allcol = cb.iloc[0].T
    # remove some columns, which will be replaced or won't be used in DDF
    noneed = ['metric', 'mean', 'unit', 'upper', 'lower']
    concepts = allcol[~allcol.index.isin(noneed)].reset_index()
    # append the measures in DDF
    concepts.columns = ['concept', 'name']
    concepts = concepts.append(
        pd.DataFrame({
            'concept': measures,
            'name': names
        }))
    concepts = concepts.append(
        pd.DataFrame([['location', 'Location'],
                      ['sex', 'Sex'],
                      ['age_group', 'Age Group']],
                     columns=['concept', 'name']))
    # set concept types
    concepts = concepts.set_index('concept')
    concepts.loc[measures, 'concept_type'] = 'measure'
    concepts.loc[['location', 'sex', 'age_group'],
                 'concept_type'] = 'entity_domain'
    concepts.loc[[
        'location_code', 'location_name', 'sex_name', 'age_group_name',
        'location_id', 'sex_id', 'age_group_id'
    ], 'concept_type'] = 'string'
    concepts.loc['year', 'concept_type'] = 'time'
    concepts.loc['name', ['concept_type', 'name']] = ['string', 'Name']

    concepts.to_csv(os.path.join(out_path, 'ddf--concepts.csv'))

    # datapackage
    # get_datapackage(out_path, use_existing=True, to_disk=True)

    print('Done.')
