*Basic example of transport model from GAMS model library

$Title  A Transportation Problem (TRNSPORT,SEQ=1)
$Ontext

This problem finds a least cost shipping schedule that meets
requirements at markets and supplies at factories.

Dantzig, G B, Chapter 3.3. In Linear Programming and Extensions.
Princeton University Press, Princeton, New Jersey, 1963.

This formulation is described in detail in:
Rosenthal, R E, Chapter 2: A GAMS Tutorial. In GAMS: A User's Guide.
The Scientific Press, Redwood City, California, 1988.

The line numbers will not match those in the book because of these
comments.

$Offtext

Sets
    i   canning plants
    j   markets
;

Parameters
    a(i)   capacity of plant i in cases
    b(j)   demand at market j in cases
    d(i,j) distance in thousands of miles
    f      freight in dollars per case per thousand miles
;

* For GAMS IDE to correctly load gdx or include files,
* a GAMS project file (*.gpr) is required.
* Please refer to the the GAMS tutorial at
* https://www.gams.com/help/index.jsp?topic=%2Fgams.doc%2Ftools%2Fgamside%2Ffmproject.htm

* This file will read in data from a gdx file and write results to another gdx file.
* The name of the input and output file can either be set directly from the command line,
* e.g. `gams transport_ixmp.gms --in\=\"<name>\" --out\=\"<name>\"`.
* If no command line parameters are given, the input and output files are set as specific below.

$IF NOT set in  $SETGLOBAL in  'ix_transport_data.gdx'
$IF NOT set out $SETGLOBAL out 'ix_transport_results.gdx'

$GDXIN '%in%'
$LOAD i, j, a, b, d, f
$GDXIN

Parameter c(i,j)  transport cost in thousands of dollars per case ;
          c(i,j) = f * d(i,j) / 1000 ;
Variables
     x(i,j)  shipment quantities in cases
     z       total transportation costs in thousands of dollars ;

Positive Variable x ;

Equations
     cost        define objective function
     supply(i)   observe supply limit at plant i
     demand(j)   satisfy demand at market j ;

cost ..        z  =e=  sum((i,j), c(i,j)*x(i,j)) ;

supply(i) ..   sum(j, x(i,j))  =l=  a(i) ;

demand(j) ..   sum(i, x(i,j))  =g=  b(j) ;

Model transport /all/ ;

Solve transport using lp minimizing z ;

Display x.l, x.m ;

Execute_unload '%out%';
