import json

def read_melb_grid(file_name):

    melb_grid = []
    
    with open(file_name, 'r') as file:
        data = json.load(file)

    for line in data['features']:
        grid = {}
        grid['id'] = line['properties']['id']
        grid['xmin'] = line['properties']['xmin']
        grid['xmax'] = line['properties']['xmax']
        grid['ymin'] = line['properties']['ymin']
        grid['ymax'] = line['properties']['ymax']
        grid['count'] = 0
        melb_grid.append(grid)

    return melb_grid

def find_melb_grid(x, y):
    grid = read_melb_grid('dataset/melbGrid.json')
    for i in grid:
        if x >= i['xmin'] and x <= i['xmax'] and y >= i['ymin'] and y <= i['ymax']:
            return i['id']
    return None

print(find_melb_grid(144.7, -37.65))
print(find_melb_grid(144.85, -37.65))
print(find_melb_grid(145.0, -37.65))
print(find_melb_grid(145.15, -37.65))
print(find_melb_grid(145.3, -37.65))

print(find_melb_grid(144.7, -37.8))
print(find_melb_grid(144.85, -37.8))
print(find_melb_grid(145.0, -37.8))
print(find_melb_grid(145.15, -37.8))
print(find_melb_grid(145.3, -37.8))

print(find_melb_grid(144.7, -37.95))
print(find_melb_grid(144.85, -37.95))
print(find_melb_grid(145.0, -37.95))
print(find_melb_grid(145.15, -37.95))
print(find_melb_grid(145.3, -37.95))

print(find_melb_grid(145.0, -38.1))
print(find_melb_grid(145.15, -38.1))
print(find_melb_grid(145.3, -38.1))