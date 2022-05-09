from satpy import available_readers, find_files_and_readers, Scene
import re
from pathlib import Path

# https://sentinels.copernicus.eu/web/sentinel/user-guides/sentinel-3-olci/naming-convention
# https://sentinels.copernicus.eu/web/sentinel/user-guides/Sentinel-3-slstr/naming-convention
pattern = re.compile('(?P<mission_id>S3[AB_])_'
                     '(?P<data_source>OL|SL)_'  # OLCI, SLSTR
                     '(?P<processing_level>0|1|2|_)_'
                     '(?P<data_type_id>[A-Z_]{6})_'  # Long list...
                     '(?P<start_datetime>[0-9]{8}T[0-9]{6})_'
                     '(?P<end_datetime>[0-9]{8}T[0-9]{6})_'
                     '(?P<product_creation_datetime>[0-9]{8}T[0-9]{6})_'
                     '(?P<instance_id>'  # 17 digits long
                        '(?P<stripes>'
                           '(?P<s_duration>[0-9]{4})_'
                           '(?P<s_cycle_number>[0-9]{3})_'
                           '(?P<s_relative_orbit_number>[0-9]{3})_{5}'
                        ')|'
                        '(?P<frames>'
                           '(?P<f_duration>[0-9]{4})_'
                           '(?P<f_cycle_number>[0-9]{3})_'
                           '(?P<f_relative_orbit_number>[0-9]{3})_'
                           '(?P<f_frame_along_track_coordinate>[0-9]{4})_'
                        ')|'
                        '(?P<tiles>'
                           '(?P<global>GLOBAL_{11})'
                           '(?P<tile_cut>[A-Za-z0-9]{17})'  # not sure about this
                        ')|'
                        '(?P<auxiliary_data>_{17})'
                     ')'
                     '(?P<generation_center>[A-Z]{3})_'
                     '(?P<class_id>'
                        '(?P<platform>O|F|D|R)_'
                        '(?P<timelines>NR|ST|NT)_'  # NR for NRT, ST for STC, NT for NTC
                        '(?P<baseline_collection>[0-9]{3})'
                     ').'
                     '(?P<filename_extension>SEN3)'
)

def walk(path: str, pattern: re.Pattern):
   # https://stackoverflow.com/questions/6639394/what-is-the-python-way-to-walk-a-directory-tree
   for p in Path(path).iterdir(): 
      if p.is_dir():
         res = pattern.match(p.name)
         if res:
            # yield from walk(p)
            yield p
         continue
      yield p.resolve()
      
scenes = list(walk('data/', pattern))
scenes_names = [s.name for s in scenes]

files = find_files_and_readers(sensor='olci',
                              base_dir='data/',
                              reader='olci_l2')

scenes_dict = {k: {} for k in scenes_names}
for k in files:
   for v in files[k]:
      key = [k for k in scenes_names if k in v][0]
      if k not in scenes_dict[key]:
         scenes_dict[key][k] = []
      if key in scenes_dict:
         scenes_dict[key][k].append(v)
      else:
         scenes_dict[key][k] = [v]
   



scn = Scene(reader='olci_l2', filenames=files)

scn.load(['Oa01'])

newscn = scn.resample(scn['Oa01'].area.compute_optimal_bb_area())
newscn.save_datasets(writer='geotiff', base_dir='data/out', compress='LZW')

scn.all_dataset_names()