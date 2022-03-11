# import numpy as np
# import matplotlib
# import matplotlib.pyplot as plt
# import matplotlib.colors as colors
import shutil
import os
import gc
import time
import snappy
import pandas as pd
from snappy import Product
from snappy import ProductIO
from snappy import ProductUtils
from snappy import WKTReader
from snappy import HashMap
from snappy import GPF
# For shapefiles
import shapefile
import pygeoif
from helper import BulkDownloader
GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis()

#path_to_sentinel_data = "data/S1A_IW_SLC__1SDV_20220116T010718_20220116T010745_041479_04EEB0_2B73.zip"
# first_product = "data/S1A_IW_SLC__1SDV_20210202T010712_20210202T010739_036404_0445E0_46D9.zip"
# second_product = "data/S1A_IW_SLC__1SDV_20210214T010712_20210214T010739_036579_044BF8_8133.zip"

# print("First Product:")
# width = first_product.getSceneRasterWidth()
# print("\tWidth: {} px".format(width))
# height = first_product.getSceneRasterHeight()
# print("\tHeight: {} px".format(height))
# name = first_product.getName()
# print("\tName: {}".format(name))
# print("Second Product:")
# width = second_product.getSceneRasterWidth()
# print("\tWidth: {} px".format(width))
# height = second_product.getSceneRasterHeight()
# print("\tHeight: {} px".format(height))
# name = second_product.getName()
# print("\tName: {}".format(name))


# def createOrthorectifiedPaths(url):
# 	filename, ext = os.path.splitext(url.split('/')[-1])
# 	topssplit_filename = f"{filename}_Top"
# 	apply_orbit_filename = f"{topssplit_filename}_Orb"
# 	topssplit_path = os.path.join(topssplit_folder, f"{topssplit_filename}.{ext}")
# 	apply_orbit_path = os.path.join(apply_orbit_folder, f"{apply_orbit_filename}.{ext}")
# 	return {"topssplit_path": topssplit_path, "apply_orbit_path": apply_orbit_path}

def createOrGetDir(directory):
	if not os.path.isdir(directory):
		os.mkdir(directory)
	return directory

def readShapefile(shpFilename):
	r = shapefile.Reader(shpFilename)
	g = []
	for s in r.shapes():
	    g.append(pygeoif.geometry.as_shape(s))
	    m = pygeoif.MultiPoint(g)
	    wkt = str(m.wkt).replace("MULTIPOINT", "POLYGON(") + ")"

	return wkt

def readProduct(productFilename, *args, **kwargs):
	print(f"Reading... {productFilename}")
	product = ProductIO.readProduct(productFilename)
	if kwargs.get('width', False):
		print(f"Width: {product.getSceneRasterWidth()} px")
	if kwargs.get('height', False):
		print(f"Height: {product.getSceneRasterHeight()} px")
	if kwargs.get('bands', False):
		print(f"Bands: {', '.join(product.getBandNames())}")

	print(f"Done\r\nProduct name: {product.getName()}")
	return product

def delProduct(pathToProduct, productName, exts=[]):
	print(f"Clearing product... {productName}")
	for ext in exts:
		try:
			name = f"{productName}.{ext}"
			path = os.path.join(pathToProduct, name)
			if os.path.isdir(path):  
				shutil.rmtree(path)
			elif os.path.isfile(path):
				os.remove(path)
			else:
				raise Exception("Could not find a valid product.")
			print(f"Successfully deleted {path}")
		except Exception as e:
			print(f"Could not delete {path}, please manually clear the product.\r\nException: {e}")

def TopsSplit(outputPath, product, suffix="Top", *args, **kwargs):
	print(f"Performing TOPSAR-Split on {product.getName()}...")
	filename = f"{product.getName()}_{suffix}"
	ext = "dim"
	targetFilename = f"{outputPath}/{filename}.{ext}"
	parameters = HashMap()
	parameters.put('selectedPolarisations', kwargs.get('polarisation', 'VV'))
	parameters.put('subswath', kwargs.get('subswath','IW2'))
	parameters.put('wktAoi', kwargs.get('aoi', ''))
	try:
		topssplit = GPF.createProduct('TOPSAR-Split', parameters, product)
		ProductIO.writeProduct(topssplit, targetFilename, "BEAM-DIMAP")
		topssplit = None
	except Exception as e:
		raise Exception(f"Problem completing operation\r\nException: {e}")
	else:
		print(f"Done\r\nProduct saved at: {targetFilename}")

	return targetFilename

def ApplyOrbit(outputPath, product, suffix="Orb", *args, **kwargs):
	print(f"Applying Orbit on {product.getName()}...")
	filename = f"{product.getName()}_{suffix}"
	ext = "dim"
	targetFilename = f"{outputPath}/{filename}.{ext}"
	parameters = HashMap()
	parameters.put("orbitType", kwargs.get('orbitType', "Sentinel Precise (Auto Download)"))
	parameters.put('polyDegree', kwargs.get('polyDegree','3'))
	parameters.put('continueOnFail', kwargs.get('contineOnFail', 'false'))
	try:
		ortho = GPF.createProduct('Apply-Orbit-File', parameters, product)
		ProductIO.writeProduct(ortho, targetFilename, "BEAM-DIMAP")
		del ortho
	except Exception as e:
		raise Exception(f"Problem completing operation\r\n{e}")
	else:
		print(f"Done\r\nProduct saved at: {targetFilename}")

	return targetFilename

def Coregistration(outputPath, first_product, second_product, suffix="Cor", *args, **kwargs):
	print(f"Coregistering {first_product.getName()}, {second_product.getName()}...")
	filename = f"{first_product.getName()}_{suffix}"
	ext = "dim"
	targetFilename = f"{outputPath}/{filename}.{ext}"
	parameters = HashMap()
	parameters.put('outputDerampDemodPhase', kwargs.get('derampDemod', 'true'))
	sourceProducts = HashMap()
	sourceProducts.put('masterProduct', first_product)
	sourceProducts.put('slaveProduct', second_product)
	try:
		coregistered_stack = GPF.createProduct('Back-Geocoding', parameters, sourceProducts)
		ProductIO.writeProduct(coregistered_stack, targetFilename, "BEAM-DIMAP")
		del coregistered_stack
	except Exception as e:
		raise Exception(f"Problem completing operation\r\n{e}")
	else:
		print(f"Done\r\nProduct saved at: {targetFilename}")

	return targetFilename

def Interferogram(outputPath, product, suffix="Inf", *args, **kwargs):
	print(f"Creating Interferogram of {product.getName()}...")
	filename = f"{product.getName()}_{suffix}"
	ext = "dim"
	targetFilename = f"{outputPath}/{filename}.{ext}"
	parameters = HashMap()
	try:
		interferogram = GPF.createProduct('Interferogram', parameters, product)
		ProductIO.writeProduct(interferogram, targetFilename, "BEAM-DIMAP")
		del interferogram
	except Exception as e:
		raise Exception(f"Problem completing operation\r\n{e}")
	else:
		print(f"Done\r\nProduct saved at: {targetFilename}")

	return targetFilename

def TopsDeburst(outputPath, product, suffix="Deb", *args, **kwargs):
	print(f"Debursting {product.getName()}...")
	filename = f"{product.getName()}_{suffix}"
	ext = "dim"
	targetFilename = f"{outputPath}/{filename}.{ext}"
	parameters = HashMap()
	try:
		deburst = GPF.createProduct('TOPSAR-Deburst', parameters, product)
		ProductIO.writeProduct(deburst, targetFilename, "BEAM-DIMAP")
		del deburst
	except Exception as e:
		raise Exception(f"Problem completing operation\r\n{e}")
	else:
		print(f"Done\r\nProduct saved at: {targetFilename}")

	return targetFilename

def TopoPhaseRemoval(outputPath, product, suffix="Phs", *args, **kwargs):
	print(f"Removing topographic phase {product.getName()}...")
	filename = f"{product.getName()}_{suffix}"
	ext = "dim"
	targetFilename = f"{outputPath}/{filename}.{ext}"
	parameters = HashMap()
	try:
		topo_phase_removed = GPF.createProduct('TopoPhaseRemoval', parameters, product)
		ProductIO.writeProduct(topo_phase_removed, targetFilename, "BEAM-DIMAP")
		del topo_phase_removed
	except Exception as e:
		raise Exception(f"Problem completing operation\r\n{e}")
	else:
		print(f"Done\r\nProduct saved at: {targetFilename}")

	return targetFilename

def Multilook(outputPath, product, suffix="Mul", *args, **kwargs):
	print(f"Multilooking {product.getName()}...")
	filename = f"{product.getName()}_{suffix}"
	ext = "dim"
	targetFilename = f"{outputPath}/{filename}.{ext}"
	parameters = HashMap()
	parameters.put('nAzLooks', '2')
	parameters.put('nRgLooks', '6')
	try:
		multilook = GPF.createProduct('Multilook', parameters, product)
		ProductIO.writeProduct(multilook, targetFilename, "BEAM-DIMAP")
		del multilook
	except Exception as e:
		raise Exception(f"Problem completing operation\r\n{e}")
	else:
		print(f"Done\r\nProduct saved at: {targetFilename}")

	return targetFilename

def GoldsteinPhaseFiltering(outputPath, product, suffix="Flt", *args, **kwargs):
	print(f"Filtering {product.getName()}...")
	filename = f"{product.getName()}_{suffix}"
	ext = "dim"
	targetFilename = f"{outputPath}/{filename}.{ext}"
	parameters = HashMap()
	try:
		phase_filtered = GPF.createProduct('GoldsteinPhaseFiltering', parameters, product)
		ProductIO.writeProduct(phase_filtered, targetFilename, "BEAM-DIMAP")
		del phase_filtered
	except Exception as e:
		raise Exception(f"Problem completing operation\r\n{e}")
	else:
		print(f"Done\r\nProduct saved at: {targetFilename}")

	return targetFilename

def Subset(outputPath, product, suffix="Sub", *args, **kwargs):
	print(f"Subsetting {product.getName()}...")
	filename = f"{product.getName()}_{suffix}"
	ext = "dim"
	targetFilename = f"{outputPath}/{filename}.{ext}"
	parameters = HashMap()
	parameters.put('copyMetadata', kwargs.get('copyMeta', True))
	parameters.put('geoRegion', kwargs.get('geometry', ''))
	try:
		subset = snappy.GPF.createProduct('Subset', parameters, product)
		ProductIO.writeProduct(subset, targetFilename, "BEAM-DIMAP")
		del subset
	except Exception as e:
		raise Exception(f"Problem completing operation\r\n{e}")
	else:
		print(f"Done\r\nProduct saved at: {targetFilename}")

	return targetFilename

def SnaphuExport(outputPath, product, suffix="Wrp", *args, **kwargs):
	print(f"Exporting {product.getName()}...")
	filename = f"{product.getName()}_{suffix}"
	targetFilename = f"{outputPath}/{filename}"
	parameters = HashMap()
	parameters.put('initMethod', 'MCF')
	parameters.put('colOverlap', '400')
	parameters.put('rowOverlap', '400')
	parameters.put('numberOfTileCols', '20')
	parameters.put('numberOfTileRows', '20')
	parameters.put('statCostMode', 'DEFO')
	parameters.put('targetFolder', 'snaphu')
	parameters.put('tileCostThreshold', '500')
	parameters.put('numberOfProcessors', '4')
	try:
		snaphu_export = GPF.createProduct('SnaphuExport', parameters, product)
		ProductIO.writeProduct(snaphu_export, targetFilename, "Snaphu")
		del snaphu_export
	except Exception as e:
		raise Exception(f"Problem completing operation\r\n{e}")
	else:
		print(f"Done\r\nProduct saved at: {targetFilename}")

	return targetFilename

def getOrthorectifiedProduct(url, shapefile, download_folder, topssplit_folder, apply_orbit_folder, topssplit_suffix, apply_orbit_suffix, ext):
	download_filename = url.split('/')[-1]
	filename, _ = os.path.splitext(download_filename)
	topssplit_filename = f"{filename}_{topssplit_suffix}"
	apply_orbit_filename = f"{topssplit_filename}_{apply_orbit_suffix}"
	download_path = os.path.join(download_folder, download_filename)
	topssplit_path = os.path.join(topssplit_folder, f"{topssplit_filename}.{ext}")
	apply_orbit_path = os.path.join(apply_orbit_folder, f"{apply_orbit_filename}.{ext}")
	if os.path.isfile(apply_orbit_path):
		return apply_orbit_path
	elif os.path.isfile(topssplit_path):
		product = readProduct(topssplit_path)
		apply_orbit_path = ApplyOrbit(apply_orbit_folder, product, apply_orbit_suffix)
		product.dispose()
		delProduct(topssplit_folder, topssplit_filename, ["dim", "data"])
		return getOrthorectifiedProduct(url, shapefile, download_folder, topssplit_folder, apply_orbit_folder, topssplit_suffix, apply_orbit_suffix, ext)
	elif os.path.isfile(download_path):
		product = readProduct(download_path)
		topssplit_path = TopsSplit(topssplit_folder, product, topssplit_suffix, aoi=shapefile)
		product.dispose()
		delProduct(download_folder, filename, ["zip"])
		return getOrthorectifiedProduct(url, shapefile, download_folder, topssplit_folder, apply_orbit_folder, topssplit_suffix, apply_orbit_suffix, ext)	
	else:
		downloader = BulkDownloader(download_folder, [url])
		downloader.download_files()
		return getOrthorectifiedProduct(url, shapefile, download_folder, topssplit_folder, apply_orbit_folder, topssplit_suffix, apply_orbit_suffix, ext)

def getSubsettedInterferogram(output_folder, geometry, master_path, slave_path, 
							  coregisteration_suffix, interferogram_suffix, 
							  deburst_suffix, phase_removal_suffix, multilook_suffix, 
							  filter_suffix, subset_suffix, ext):
	master_filename, _ = os.path.splitext(master_path.split('\\')[-1])
	coregistered_filename = f"{master_filename}_{coregisteration_suffix}"
	coregistered_path = os.path.join(output_folder, f"{coregistered_filename}.{ext}")
	interferogram_filename = f"{coregistered_filename}_{interferogram_suffix}" 
	interferogram_path = os.path.join(output_folder, f"{interferogram_filename}.{ext}")
	deburst_filename = f"{interferogram_filename}_{deburst_suffix}" 
	deburst_path = os.path.join(output_folder, f"{deburst_filename}.{ext}")
	phase_removal_filename = f"{deburst_filename}_{phase_removal_suffix}" 
	phase_removal_path = os.path.join(output_folder, f"{phase_removal_filename}.{ext}")
	multilook_filename = f"{phase_removal_filename}_{multilook_suffix}" 
	multilook_path = os.path.join(output_folder, f"{multilook_filename}.{ext}")
	filter_filename = f"{multilook_filename}_{filter_suffix}" 
	filter_path = os.path.join(output_folder, f"{filter_filename}.{ext}")
	subset_filename = f"{filter_filename}_{subset_suffix}" 
	subset_path = os.path.join(output_folder, f"{subset_filename}.{ext}")
	if os.path.isfile(subset_path):
		return subset_path
	elif os.path.isfile(filter_path):
		product = readProduct(filter_path)
		subset_path = Subset(output_folder, product, subset_suffix, geometry=geometry)
		product.dispose()
		del product
		delProduct(output_folder, filter_filename, ["dim", "data"])
		return getSubsettedInterferogram(output_folder, geometry, master_path, slave_path, 
									  	 coregisteration_suffix, interferogram_suffix, 
									  	 deburst_suffix, phase_removal_suffix, multilook_suffix, 
									  	 filter_suffix, subset_suffix, ext)
	elif os.path.isfile(multilook_path):
		product = readProduct(multilook_path)
		filter_path = GoldsteinPhaseFiltering(output_folder, product, filter_suffix)
		product.dispose()
		del product
		delProduct(output_folder, multilook_filename, ["dim", "data"])
		return getSubsettedInterferogram(output_folder, geometry, master_path, slave_path, 
									  	 coregisteration_suffix, interferogram_suffix, 
									  	 deburst_suffix, phase_removal_suffix, multilook_suffix, 
									  	 filter_suffix, subset_suffix, ext)
	elif os.path.isfile(phase_removal_path):
		product = readProduct(phase_removal_path)
		multilook_path = Multilook(output_folder, product, multilook_suffix)
		product.dispose()
		del product
		delProduct(output_folder, phase_removal_filename, ["dim", "data"])
		return getSubsettedInterferogram(output_folder, geometry, master_path, slave_path, 
									  	 coregisteration_suffix, interferogram_suffix, 
									  	 deburst_suffix, phase_removal_suffix, multilook_suffix, 
									  	 filter_suffix, subset_suffix, ext)
	elif os.path.isfile(deburst_path):
		product = readProduct(deburst_path)
		phase_removal_path = TopoPhaseRemoval(output_folder, product, phase_removal_suffix)
		product.dispose()
		del product
		delProduct(output_folder, deburst_filename, ["dim", "data"])
		return getSubsettedInterferogram(output_folder, geometry, master_path, slave_path, 
									  	 coregisteration_suffix, interferogram_suffix, 
									  	 deburst_suffix, phase_removal_suffix, multilook_suffix, 
									  	 filter_suffix, subset_suffix, ext)
	elif os.path.isfile(interferogram_path):
		product = readProduct(interferogram_path)
		deburst_path = TopsDeburst(output_folder, product, deburst_suffix)
		product.dispose()
		del product
		delProduct(output_folder, interferogram_filename, ["dim", "data"])
		return getSubsettedInterferogram(output_folder, geometry, master_path, slave_path, 
									  	 coregisteration_suffix, interferogram_suffix, 
									  	 deburst_suffix, phase_removal_suffix, multilook_suffix, 
									  	 filter_suffix, subset_suffix, ext)
	elif os.path.isfile(coregistered_path):
		product = readProduct(coregistered_path)
		interferogram_path = Interferogram(output_folder, product, interferogram_suffix)
		product.dispose()
		del product
		delProduct(output_folder, coregistered_filename, ["dim", "data"])
		return getSubsettedInterferogram(output_folder, geometry, master_path, slave_path, 
									  	 coregisteration_suffix, interferogram_suffix, 
									  	 deburst_suffix, phase_removal_suffix, multilook_suffix, 
									  	 filter_suffix, subset_suffix, ext)
	else:
		master_product = readProduct(master_path)
		slave_product = readProduct(slave_path)
		coregistered_path = Coregistration(output_folder, master_product, slave_product, coregisteration_suffix)
		master_product.dispose()
		slave_product.dispose()
		del master_product
		del slave_product
		return getSubsettedInterferogram(output_folder, geometry, master_path, slave_path, 
									  	 coregisteration_suffix, interferogram_suffix, 
									  	 deburst_suffix, phase_removal_suffix, multilook_suffix, 
									  	 filter_suffix, subset_suffix, ext)

def main():
	shpFilename = "data/shapefiles/mansehra_balakot_muzaffarabad.shp"
	shapeFile = readShapefile(shpFilename)
	geometry = WKTReader().read(shapeFile)
	download_folder = createOrGetDir("sar_downloads")
	apply_orbit_folder = createOrGetDir("orthorectified")
	topssplit_folder = createOrGetDir("topsSplit")
	interferogram_folder = createOrGetDir("interferograms")
	snaphu_folder = createOrGetDir("snaphu")
	apply_orbit_suffix = "Orb"
	topssplit_suffix = "Top"
	coregisteration_suffix = "Cor"
	interferogram_suffix = "Inf"
	deburst_suffix = "Deb"
	phase_removal_suffix = "Phs"
	multilook_suffix = "Mul"
	filter_suffix = "Flt"
	subset_suffix = "Sub"
	pairs_csv = pd.read_csv(f"data/SAR_SBAS.csv", header=0, 
							usecols=["Reference", " Reference URL", "Secondary", " Secondary URL"], 
							dtype=object, squeeze=True)
	pairs_csv = pairs_csv.rename(columns={"Reference": "Master", " Reference URL": "MasterURL", 
										  "Secondary": "Slave", " Secondary URL": "SlaveURL"})
	for i, entry  in pairs_csv.iterrows():
		masterPath = getOrthorectifiedProduct(entry["MasterURL"], shapeFile, download_folder, topssplit_folder, apply_orbit_folder, 
											  topssplit_suffix, apply_orbit_suffix, "dim")
		slavePath = getOrthorectifiedProduct(entry["SlaveURL"], shapeFile, download_folder, topssplit_folder, apply_orbit_folder, 
											 topssplit_suffix, apply_orbit_suffix, "dim")
		interferogramSubsetPath = getSubsettedInterferogram(interferogram_folder, geometry, masterPath, slavePath, 
															coregisteration_suffix, interferogram_suffix, deburst_suffix, 
															phase_removal_suffix, multilook_suffix, filter_suffix, 
															subset_suffix, "dim")
		gc.collect()

	
	# shpFilename = "data/shapefiles/mansehra_balakot_muzaffarabad.shp"
	# shapeFile = readShapefile(shpFilename)
	# setOfSAR = ["data/S1A_IW_SLC__1SDV_20210202T010712_20210202T010739_036404_0445E0_46D9.zip", "data/S1A_IW_SLC__1SDV_20210202T010712_20210202T010739_036404_0445E0_46D9.zip"] 
	# for productFilename in setOfSAR:
	# 	product = readProduct(productFilename, width=True, height=True)
	# 	topsSpitFilename = TopsSplit("topsSplit", product, aoi=shapeFile)
	# 	product = readProduct(topsSpitFilename, bands=True)
	# 	orthoFilename = ApplyOrbit("orbitApplied", product)



	# geocoded = BackGeocoding()
	# downloader = BulkDownloader("sar_downloads", ["https://datapool.asf.alaska.edu/SLC/SA/S1A_IW_SLC__1SDV_20200103T010707_20200103T010734_030629_038274_52BF.zip"])
	# downloader.download_files()
	# downloader.print_summary()


if __name__ == "__main__":
    start_time = time.time()
    main()
    print(f"--- {time.time() - start_time} seconds ---")