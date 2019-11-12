"""
Test check_year(y,s) 

@author: unlu
"""
from ixmp.utils import check_year
import pytest

"Enter a string value for y to raise a ValueError. - in this case test should be successfull"
 
def test_check_year():
   
    y1= "a"
    s1= "a"
      
    with pytest.raises(ValueError):
        assert check_year(y1,s1)
    
    
    "If y = None."
        
    y2 = None
    s2 = None 
    
    assert check_year(y2,s2) == None
        
    "If y is integer."
        
    y3= 4
    s3= 4
  
    assert check_year(y3,s3) == True
        
    
    
    
    

