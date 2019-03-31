import unittest
import helper

class TestStringMethods(unittest.TestCase):
    
    def test_melb_grid(self):

        melb_grid = helper.MelbGrid('dataset/melbGrid.json')

        self.assertEqual(melb_grid.grid_idx_to_id(melb_grid.find_grid_idx(144.7, -37.65)),  'A1')
        self.assertEqual(melb_grid.grid_idx_to_id(melb_grid.find_grid_idx(144.85, -37.65)), 'A1')
        self.assertEqual(melb_grid.grid_idx_to_id(melb_grid.find_grid_idx(145.0, -37.65)),  'A2')
        self.assertEqual(melb_grid.grid_idx_to_id(melb_grid.find_grid_idx(145.15, -37.65)), 'A3')
        self.assertEqual(melb_grid.grid_idx_to_id(melb_grid.find_grid_idx(145.3, -37.65)),  'A4')

        self.assertEqual(melb_grid.grid_idx_to_id(melb_grid.find_grid_idx(144.7, -37.8)),   'B1')
        self.assertEqual(melb_grid.grid_idx_to_id(melb_grid.find_grid_idx(144.85, -37.8)),  'B1')
        self.assertEqual(melb_grid.grid_idx_to_id(melb_grid.find_grid_idx(145.0, -37.8)),   'B2')
        self.assertEqual(melb_grid.grid_idx_to_id(melb_grid.find_grid_idx(145.15, -37.8)),  'B3')
        self.assertEqual(melb_grid.grid_idx_to_id(melb_grid.find_grid_idx(145.3, -37.8)),   'B4')

        self.assertEqual(melb_grid.grid_idx_to_id(melb_grid.find_grid_idx(144.7, -37.95)),  'C1')
        self.assertEqual(melb_grid.grid_idx_to_id(melb_grid.find_grid_idx(144.85, -37.95)), 'C1')
        self.assertEqual(melb_grid.grid_idx_to_id(melb_grid.find_grid_idx(145.0, -37.95)),  'C2')
        self.assertEqual(melb_grid.grid_idx_to_id(melb_grid.find_grid_idx(145.15, -37.95)), 'C3')
        self.assertEqual(melb_grid.grid_idx_to_id(melb_grid.find_grid_idx(145.3, -37.95)),  'C4')

        self.assertEqual(melb_grid.grid_idx_to_id(melb_grid.find_grid_idx(145.0, -38.1)),   'D3')
        self.assertEqual(melb_grid.grid_idx_to_id(melb_grid.find_grid_idx(145.15, -38.1)),  'D3')
        self.assertEqual(melb_grid.grid_idx_to_id(melb_grid.find_grid_idx(145.3, -38.1)),   'D4')

        print('Assert MelbGrid Done.')
    

if __name__ == '__main__':
    unittest.main()