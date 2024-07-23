from pystac_client import Client
import geopandas as gpd
from pyproj import CRS
import rioxarray
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap, BoundaryNorm
from datetime import datetime, timedelta
from dateutil.tz import tzutc
from shapely.geometry import mapping



class Sentinel2Data:
    def __init__(self, api_url="https://earth-search.aws.element84.com/v1", collection="sentinel-2-l2a"):
        self.client=self.setup_client(api_url)
        self.collection=collection
        
    def setup_client(self,api_url=None):
        client=Client.open(api_url)
        return client 
    
    def fetch_data(self,date,boundary):
        s2_date = datetime.strptime(date, "%Y%m%d").replace(tzinfo=tzutc())

        start_date = (s2_date - timedelta(days=6)).strftime("%Y-%m-%d")
        end_date = (s2_date + timedelta(days=6)).strftime("%Y-%m-%d")
        date_range = f"{start_date}/{end_date}"

        search = self.client.search(
            collections=[self.collection],
            intersects=boundary.geometry[0],
            datetime=date_range,
            # query={'eo:cloud_cover': {'lt': 30}},

        )
        s2_items = search.item_collection()
        if not s2_items:
          print("No data found")
          return None, None
        
        s2_date_item = self.fetch_date(s2_date, s2_items)
        dt = str(s2_date_item).split('_')[-3]

        asset = s2_date_item.assets
        return dt,asset
    
    def fetch_date(self,dt,items):
        s2_item_dates = []
        for item in items:
            item_date = item.datetime
            s2_item_dates.append((item_date, item))
        nearest_dt_item = min(s2_item_dates, key=lambda x: abs(x[0] - dt))[1]
        return nearest_dt_item
    
    def ndmi_indices(self,nir,swir):
        ndmi = (nir-swir)/(nir+swir)
        return ndmi
    
    def band_index_clip(self,collection,nir_band,swir_band,bndry,indices='ndmi'):
        swir_uri = collection[nir_band].href
        nir_uri = collection[swir_band].href
        
        swir_band= rioxarray.open_rasterio(swir_uri,masked=True)
        nir_band = rioxarray.open_rasterio(nir_uri,masked=True)
        
        bndry.to_crs(CRS(swir_band.rio.crs),inplace =True)
        xmin,miny,maxx,maxy = bndry.total_bounds
        
        nir_clip = nir_band.rio.clip_box(*bndry.total_bounds)
        swir_clip = swir_band.rio.clip_box(*bndry.total_bounds)
        
        if not nir_clip.rio.resolution()[0] ==  swir_clip.rio.resolution()[0]:
            swir_clip = swir_clip.rio.reproject_match(nir_clip)
        
        ndmi_ras = self.ndmi_indices(nir_clip,swir_clip)

        ndmi_val_mask = ndmi_ras.rio.clip(bndry.geometry.apply(mapping), bndry.crs, drop=False, invert=False,all_touched=True)
        return ndmi_val_mask
    
    def mean(self,ndmi_data):
        mean_val = np.nanmean(ndmi_data[0].data)
        return round(float(mean_val), 3)
        
    def generate_png(self,data,bndry,dt,png_path=None):

        Moisture_index_color = {
            '0,0.2': '#f29e63',
            '0.2,0.4': '#f4f2ae',
            '0.4,0.6': '#bfcfea',
            '0.6,0.8': '#507dd2',
            '0.8,1': '#08306b'}
        
        bounds = [0, 0.2, 0.4, 0.6, 0.8, 1]
        cmap = ListedColormap([Moisture_index_color[key] for key in sorted(Moisture_index_color.keys())])
        norm = BoundaryNorm(bounds, cmap.N)
        
        fig, ax = plt.subplots(figsize=(10, 10))
        data.plot(ax=ax, cmap=cmap, norm=norm)
        bndry.plot(ax=ax, facecolor='none', edgecolor='red')
        ax.set_title(dt)
        ax.set_xlabel('')
        ax.set_ylabel('')
        ax.set_xticks([])
        ax.set_yticks([])
        if png_path is not None:
            plt.savefig(png_path, bbox_inches='tight', pad_inches=0, transparent=True)
        plt.show()
        plt.close(fig)
        
    def process(self, date_str=None, boundary=None, png_path=None, indices='ndmi'):
        gdf= gpd.read_file(boundary) 
        dt,date_item=self.fetch_data(date_str, gdf)
        if dt is None:
            print("No data found")
            return
        res=self.band_index_clip(date_item,'swir16','nir',gdf,indices=indices)
            
        avg=self.mean(res)
        print(f'Mean NDMI value for {dt} is {avg}')
        self.generate_png(res,gdf,dt,png_path=png_path)
        print('Processed!') 
        
        
    
    
sent=Sentinel2Data()
sent.process(date_str='20240723',boundary='/home/dineshkumar/Projects/temp/tmp/farm_polygon.geojson')    
  
    
