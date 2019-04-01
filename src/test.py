import unittest
from helper import *

class TestStringMethods(unittest.TestCase):
    
    def test_melb_grid(self):

        melb_grid = MelbGrid('dataset/melbGrid.json')

        grid_list = [
            ((144.70, -37.65), 'A1'),
            ((144.85, -37.65), 'A1'),
            ((145.00, -37.65), 'A2'),
            ((145.15, -37.65), 'A3'),
            ((145.30, -37.65), 'A4'),
            ((144.70, -37.80), 'B1'),
            ((144.85, -37.80), 'B1'),
            ((145.00, -37.80), 'B2'),
            ((145.15, -37.80), 'B3'),
            ((145.30, -37.80), 'B4'),
            ((144.70, -37.95), 'C1'),
            ((144.85, -37.95), 'C1'),
            ((145.00, -37.95), 'C2'),
            ((145.15, -37.95), 'C3'),
            ((145.30, -37.95), 'C4'),
            ((145.00, -38.10), 'D3'),
            ((145.15, -38.10), 'D3'),
            ((145.30, -38.10), 'D4')
        ]

        for i in grid_list:
            idx = melb_grid.find_grid_idx(i[0][0], i[0][1])
            self.assertEqual(melb_grid.grid_idx_to_id(idx), i[1])

        print('Assert MelbGrid Done.')


if __name__ == '__main__':
    unittest.main()