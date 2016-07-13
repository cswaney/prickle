# -*- coding: utf-8 -*-

from .context import hfttools as hft

import unittest
import os


class BasicTestSuite(unittest.TestCase):
    """Basic test cases."""

    def test_absolute_truth_and_meaning(self):
        assert True


if __name__ == '__main__':
    unittest.main()


# Initialize a Database
db = hft.Database(path=os.path.abspath('..'), names=['GOOG'])
db.close()

# Initialize an Orderlist.
orderlist = hft.Orderlist()

# Initialize a Book.
book = hft.Book(5)

# Add orders
print('Adding orders...')
add_message = hft.Message(sec=1, nano=1, type='A', name='GOOG', buysell='B',
                          price=400, shares=100, refno=1)
print('Message:\n{}'.format(add_message))
orderlist.add(add_message)
print('Orderlist:\n{}'.format(orderlist))
book.update(add_message)
print('Book:\n{}'.format(book))
add_message = hft.Message(sec=1, nano=1, type='F', name='GOOG', buysell='S',
                          price=410, shares=200, refno=2)
print('Message:\n{}'.format(add_message))
orderlist.add(add_message)
print('Orderlist:\n{}'.format(orderlist))
book.update(add_message)
print('Book:\n{}'.format(book))

# Delete an order
print('Deleting an order...')
delete_message = hft.Message(sec=1, nano=1, type='D', refno=1)
print('Message:\n{}'.format(delete_message))
orderlist.complete_message(delete_message)
print('Message:\n{}'.format(delete_message))
orderlist.update(delete_message)
print('Orderlist:\n{}'.format(orderlist))
book.update(delete_message)
print('Book:\n{}'.format(book))

# Cancel an order
print('Cancelling an order...')
cancel_message = hft.Message(sec=1, nano=1, type='C', shares=10, refno=2)
print('Message:\n{}'.format(cancel_message))
orderlist.complete_message(cancel_message)
print('Message:\n{}'.format(cancel_message))
orderlist.update(cancel_message)
print('Orderlist:\n{}'.format(orderlist))
book.update(cancel_message)
print('Book:\n{}'.format(book))

# Execute an order
print('Executing an order...')
execute_message = hft.Message(sec=1, nano=1, type='E', shares=50, refno=2)
print('Message:\n{}'.format(execute_message))
orderlist.complete_message(execute_message)
print('Message:\n{}'.format(execute_message))
orderlist.update(execute_message)
print('Orderlist:\n{}'.format(orderlist))
book.update(execute_message)
print('Book:\n{}'.format(book))

# Replace an order
print('Replacing an order...')
replace_message = hft.Message(sec=1, nano=1, type='U', price=405, shares=100,
                              refno=2, newrefno=3)
print('Message:\n{}'.format(replace_message))
delete_message, add_message = replace_message.split()
print('Message:\n{}'.format(delete_message))
print('Message:\n{}'.format(add_message))
orderlist.complete_message(delete_message)
orderlist.complete_message(add_message)
print('Message:\n{}'.format(delete_message))
print('Message:\n{}'.format(add_message))
orderlist.update(delete_message)
orderlist.update(add_message)
print('Orderlist:\n{}'.format(orderlist))
book.update(delete_message)
book.update(add_message)
print('Book:\n{}'.format(book))
