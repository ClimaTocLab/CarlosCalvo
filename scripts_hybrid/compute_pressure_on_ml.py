import metview as mv
import sys
  
data = mv.read(sys.argv[1])
lnsp = data.select(level=1,shortName='lnsp')
p = mv.unipressure(lnsp)
mv.write(sys.argv[2],p)
