# -*- coding: utf-8 -*-

import os
import pandas as pd
from ddf_utils.str import to_concept_id
from ddf_utils.index import get_datapackage

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
    # for each measure, and we want each age group a separate indicator.
    # So we will loop though each metric/age group combination to generate
    # indicators.
    for met in cb.metric.drop(0).dropna().unique():
        met_id = to_concept_id(met)
        d1 = data.groupby(by='metric').get_group(met_id)
        for g, idxs in d1.groupby('age_group_name').groups.items():
            df = d1.ix[idxs].copy()
            for i in ['mean', 'lower', 'upper']:
                # append the measure and measure name lists.
                measure = '{}_{}_{}'.format(met_id, to_concept_id(g), i)
                measures.append(measure)
                if i in ['lower', 'upper']:
                    name = '{}, Age {}: 95% Uncertainty Interval - {} Bound'.format(
                        met, g, i.title())
                else:
                    name = '{}, Age {}: Mean'.format(met, g)
                names.append(name)

                # save datapoints
                df = df.rename(columns={i: measure})
                df[['location_id', 'year', 'sex_id', measure]].to_csv(
                    os.path.join(
                        out_path,
                        'ddf--datapoints--{}--by--location_id-sex_id--year.csv'.
                        format(measure)),
                    index=False,
                    float_format='%.2f')

    # entities
    loc = data[['location_id', 'location_code',
                'location_name']].drop_duplicates()
    loc.to_csv(
        os.path.join(out_path, 'ddf--entities--location_id.csv'), index=False)
    sex = data[['sex_id', 'sex_name']].drop_duplicates()
    sex.to_csv(
        os.path.join(out_path, 'ddf--entities--sex_id.csv'), index=False)

    # concepts
    allcol = cb.ix[0].T
    # remove some columns, which will be replaced or won't be used in DDF
    noneed = [
        'age_group_id', 'age_group_name', 'metric', 'mean', 'unit', 'upper',
        'lower'
    ]
    concepts = allcol[~allcol.index.isin(noneed)].reset_index()
    # append the measures in DDF
    concepts.columns = ['concept', 'name']
    concepts = concepts.append(
        pd.DataFrame({
            'concept': measures,
            'name': names
        }))
    # set concept types
    concepts = concepts.set_index('concept')
    concepts.ix[measures, 'concept_type'] = 'measure'
    concepts.ix[['location_id', 'sex_id'], 'concept_type'] = 'entity_domain'
    concepts.ix[['location_code', 'location_name', 'sex_name'],
                'concept_type'] = 'string'
    concepts.ix['year', 'concept_type'] = 'time'
    concepts.ix['name', ['concept_type', 'name']] = ['string', 'Name']

    concepts.to_csv(os.path.join(out_path, 'ddf--concepts.csv'))

    # datapackage
    get_datapackage(out_path, use_existing=True, to_disk=True)

    print('Done.')
