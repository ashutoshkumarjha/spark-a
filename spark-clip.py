from shapely.geometry import shape, Point
from pyspark import SparkContext
import json

INPUT_FILE = './data/sample-data.csv'
OUTPUT_DIR = './data/output'
GEOJSON_FILE = './data/GHA.geojson'
GEOJSON_PROPERTY_KEY = 'ISO'
GEOJSON_PROPERTY_VALUE = 'GHA'

# Return True or False
def point_in_poly(row,poly):
	row = row.split(',')
	p = Point(float(row[1]), float(row[2]))
	return poly.contains(p)

def getShape():
	with open(GEOJSON_FILE) as data_file:
		feature_collection = json.loads(data_file.read())['features']
		filtered = filter(lambda f: f['properties'][GEOJSON_PROPERTY_KEY] == GEOJSON_PROPERTY_VALUE, feature_collection)
		for feature in filtered:
			poly = shape(feature['geometry'])
			return poly

def main():
	poly = getShape()
	sc = SparkContext()
	lines = sc.textFile(INPUT_FILE)
	feature_points = lines.filter(lambda j: point_in_poly(j, poly))
	feature_points.saveAsTextFile(OUTPUT_DIR)

if __name__ == '__main__':
	main()
