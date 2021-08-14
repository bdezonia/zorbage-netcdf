/*
 * zorbage-netcdf: code for using the NetCDF data file library to open files into zorbage data structures for further processing
 *
 * Copyright (C) 2020-2021 Barry DeZonia
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package nom.bdezonia.zorbage.netcdf;

import java.io.IOException;
import java.util.List;

import nom.bdezonia.zorbage.algebra.Algebra;
import nom.bdezonia.zorbage.algebra.Allocatable;
import nom.bdezonia.zorbage.algebra.G;
import nom.bdezonia.zorbage.algorithm.GridIterator;
import nom.bdezonia.zorbage.misc.DataBundle;
import nom.bdezonia.zorbage.data.DimensionedDataSource;
import nom.bdezonia.zorbage.data.DimensionedStorage;
import nom.bdezonia.zorbage.dataview.OneDView;
import nom.bdezonia.zorbage.dataview.PlaneView;
import nom.bdezonia.zorbage.dataview.TwoDView;
import nom.bdezonia.zorbage.procedure.Procedure3;
import nom.bdezonia.zorbage.sampling.IntegerIndex;
import nom.bdezonia.zorbage.sampling.SamplingIterator;
import nom.bdezonia.zorbage.tuple.Tuple2;
import nom.bdezonia.zorbage.type.character.CharMember;
import nom.bdezonia.zorbage.type.integer.int1.UnsignedInt1Member;
import nom.bdezonia.zorbage.type.integer.int16.SignedInt16Member;
import nom.bdezonia.zorbage.type.integer.int16.UnsignedInt16Member;
import nom.bdezonia.zorbage.type.integer.int32.SignedInt32Member;
import nom.bdezonia.zorbage.type.integer.int32.UnsignedInt32Member;
import nom.bdezonia.zorbage.type.integer.int64.SignedInt64Member;
import nom.bdezonia.zorbage.type.integer.int64.UnsignedInt64Member;
import nom.bdezonia.zorbage.type.integer.int8.SignedInt8Member;
import nom.bdezonia.zorbage.type.integer.int8.UnsignedInt8Member;
import nom.bdezonia.zorbage.type.real.float32.Float32Member;
import nom.bdezonia.zorbage.type.real.float64.Float64Member;
import nom.bdezonia.zorbage.type.string.FixedStringMember;
import ucar.ma2.Array;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFiles;
import ucar.nc2.Variable;

/**
 * 
 * @author Barry DeZonia
 *
 */
public class NetCDF {

	/**
	 * 
	 * @param filename
	 * @return
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public static DataBundle loadAllDatasets(String filename) {
		DataBundle bundle = new DataBundle();
		try {
			NetcdfFile file = NetcdfFiles.open(filename);
			
			List<Variable> vars = file.getVariables();

			for (Variable var : vars) {
			
				Tuple2<Algebra<?,?>, DimensionedDataSource<?>> dataSource = readVar(var, filename);
				
				if (dataSource == null)
					continue;
				
				Object type = dataSource.a().construct();
				
				if (type instanceof UnsignedInt1Member) {
					bundle.mergeUInt1((DimensionedDataSource<UnsignedInt1Member>) dataSource.b());
				}
				else if (type instanceof UnsignedInt8Member) {
					bundle.mergeUInt8((DimensionedDataSource<UnsignedInt8Member>) dataSource.b());
				}
				else if (type instanceof SignedInt8Member) {
					bundle.mergeInt8((DimensionedDataSource<SignedInt8Member>) dataSource.b());
				}
				else if (type instanceof UnsignedInt16Member) {
					bundle.mergeUInt16((DimensionedDataSource<UnsignedInt16Member>) dataSource.b());
				}
				else if (type instanceof SignedInt16Member) {
					bundle.mergeInt16((DimensionedDataSource<SignedInt16Member>) dataSource.b());
				}
				else if (type instanceof UnsignedInt32Member) {
					bundle.mergeUInt32((DimensionedDataSource<UnsignedInt32Member>) dataSource.b());
				}
				else if (type instanceof SignedInt32Member) {
					bundle.mergeInt32((DimensionedDataSource<SignedInt32Member>) dataSource.b());
				}
				else if (type instanceof UnsignedInt64Member) {
					bundle.mergeUInt64((DimensionedDataSource<UnsignedInt64Member>) dataSource.b());
				}
				else if (type instanceof SignedInt64Member) {
					bundle.mergeInt64((DimensionedDataSource<SignedInt64Member>) dataSource.b());
				}
				else if (type instanceof Float32Member) {
					bundle.mergeFlt32((DimensionedDataSource<Float32Member>) dataSource.b());
				}
				else if (type instanceof Float64Member) {
					bundle.mergeFlt64((DimensionedDataSource<Float64Member>) dataSource.b());
				}
				else if (type instanceof FixedStringMember) {
					bundle.mergeFixedString((DimensionedDataSource<FixedStringMember>) dataSource.b());
				}
				else if (type instanceof CharMember) {
					bundle.mergeChar((DimensionedDataSource<CharMember>) dataSource.b());
				}
				else {
					String dataType = var.getDataType().toString();
					System.out.println("Ignoring unknown data type : " + dataType);
				}
			}
		}
		catch (IOException e) {
			System.out.println("Exception occured : " + e);
		}
		return bundle;
	}
	
	@SuppressWarnings({"unchecked", "rawtypes", "deprecation"})
	private static Tuple2<Algebra<?,?>, DimensionedDataSource<?>> readVar(Variable var, String filename) {

		// TODO try as I might I cannot find any info about axis calibrations/scales/offsets.
		// I did find a web page that says some people encode annotations as "scale_factor"
		// and "add_offset" but I'm not finding them in at least some of my data. Ask in
		// community how to find this info.
		
		int rank = var.getRank();
		
		long[] dims = new long[var.getRank()];
		String[] axisLabels = new String[var.getRank()];
		for (int i = 0; i < dims.length; i++) {
			dims[i] = var.getDimension(i).getLength();
			axisLabels[i] = var.getDimension(i).getShortName();
		}
		
		// now fix dims and units in various ways to undo some netcdf weirdness
		
		// never allocate a rank 0 DS, treats as single number rank 1 list of 1 element
		if (rank == 0) {
			dims = new long[] {1};
			axisLabels = new String[] {"d0"};
		}
		
		// reverse dims because coord systems differ

		long[] tmpDims = dims.clone();
		String[] tmpLabels = axisLabels.clone();
		for (int i = 0; i < dims.length; i++) {
			tmpDims[dims.length-1-i] = dims[i];
			tmpLabels[dims.length-1-i] = axisLabels[i];
		}
		dims = tmpDims;
		axisLabels = tmpLabels;
		
		// remove coords with size of 1 when its position is beyond x or y axes
		
		tmpDims = dims.clone();
		tmpLabels = axisLabels.clone();
		int numToKill = 0;
		for (int i = 2; i < dims.length; i++) {
			if (dims[i] == 1)
				numToKill++;
		}
		if (numToKill > 0) {
			tmpDims = new long[dims.length - numToKill];
			tmpLabels = new String[dims.length - numToKill];
			int valid = 0;
			if (dims.length > 0) {
				tmpDims[0] = dims[0];
				tmpLabels[0] = axisLabels[0];
				valid++;
			}
			if (dims.length > 1) {
				tmpDims[1] = dims[1];
				tmpLabels[1] = axisLabels[1];
				valid++;
			}
			for (int i = 2; i < dims.length; i++) {
				if (dims[i] != 1) {
					tmpDims[valid] = dims[i];
					tmpLabels[valid] = axisLabels[i];
					valid++;
				}
			}
		}
		dims = tmpDims;
		axisLabels = tmpLabels;

		String dataType = var.getDataType().toString();
		
		Algebra<?,Allocatable> algebra = zorbageAlgebra(dataType);
		
		if (algebra == null) {
			
			System.out.println("Cannot determine how to import "+dataType+". Ignoring dataSource "+var.getFullName()+".");
			
			return null;
		}		
		
		Allocatable type = algebra.construct(); 
		
		Procedure3<Array,Integer,Object> converterProc =
				(Procedure3<Array,Integer,Object>) converter(var.getDataType().toString());
		
		DimensionedDataSource<Object> dataSource =
				DimensionedStorage.allocate(type, dims); 

		importValues(algebra, var, converterProc, dataSource);
		
		dataSource.setName(var.getNameAndDimensions());
		
		dataSource.setSource(filename);

		dataSource.setValueUnit(var.getUnitsString());
		
		for (int i = 0; i < axisLabels.length; i++) {
			dataSource.setAxisType(i, axisLabels[i]);
		}
		
		return new Tuple2<>(algebra, dataSource);
	}

	@SuppressWarnings("unchecked")
	private static <T extends Algebra<T,U>, U extends Allocatable<U>>
		T zorbageAlgebra(String netcdfType)
	{
		
		if (netcdfType.equalsIgnoreCase("String"))
			return (T) G.FSTRING;
		
		if (netcdfType.equals("char"))
			return (T) G.CHAR;
		
		if (netcdfType.equals("boolean"))
			return (T) G.UINT1;
		
		if (netcdfType.equals("byte") || netcdfType.equals("enum1"))
			return (T) G.INT8;
		
		if (netcdfType.equals("ubyte"))
			return (T) G.UINT8;
		
		if (netcdfType.equals("short") || netcdfType.equals("enum2"))
			return (T) G.INT16;
		
		if (netcdfType.equals("ushort"))
			return (T) G.UINT16;
		
		if (netcdfType.equals("int") || netcdfType.equals("enum4"))
			return (T) G.INT32;
		
		if (netcdfType.equals("uint"))
			return (T) G.UINT32;
		
		if (netcdfType.equals("long"))
			return (T) G.INT64;
		
		if (netcdfType.equals("ulong"))
			return (T) G.UINT64;
		
		if (netcdfType.equals("float"))
			return (T) G.FLT;
		
		if (netcdfType.equals("double"))
			return (T) G.DBL;

		System.out.println("No algebra can be found for "+netcdfType);
		
		return null;
	}

	private static Procedure3<Array,Integer,?> converter(String netcdfType) {
		
		if (netcdfType.equals("char"))
			return new Procedure3<Array, Integer, CharMember>() {
			
				@Override
				public void call(Array arr, Integer i, CharMember out) {
					char ch = arr.getChar(i);
					//System.out.println("READ '"+ch+"' from position "+i+" and am storing it");
					out.setV(ch);
				}
			
			};
		
		if (netcdfType.equalsIgnoreCase("String"))
			return new Procedure3<Array, Integer, FixedStringMember>() {
			
				@Override
				public void call(Array arr, Integer i, FixedStringMember out) {
					String result = arr.toString();
					out.setV(result);
				}
			
			};
			
		if (netcdfType.equals("boolean"))
			return new Procedure3<Array, Integer, UnsignedInt1Member>() {
			
				@Override
				public void call(Array arr, Integer i, UnsignedInt1Member out) {
					out.setV(arr.getBoolean(i) ? 1 : 0);
				}
			
			};
		
		if (netcdfType.equals("byte") || netcdfType.equals("enum1"))
			return new Procedure3<Array, Integer, SignedInt8Member>() {
			
				@Override
				public void call(Array arr, Integer i, SignedInt8Member out) {
					out.setV(arr.getByte(i));
				}
			
			};
		
		if (netcdfType.equals("ubyte"))
			return new Procedure3<Array, Integer, UnsignedInt8Member>() {
			
				@Override
				public void call(Array arr, Integer i, UnsignedInt8Member out) {
					out.setV(arr.getByte(i));
				}
			
			};
		
		if (netcdfType.equals("short") || netcdfType.equals("enum2"))
			return new Procedure3<Array, Integer, SignedInt16Member>() {
			
				@Override
				public void call(Array arr, Integer i, SignedInt16Member out) {
					out.setV(arr.getShort(i));
				}
			
			};
		
		if (netcdfType.equals("ushort"))
			return new Procedure3<Array, Integer, UnsignedInt16Member>() {
			
				@Override
				public void call(Array arr, Integer i, UnsignedInt16Member out) {
					out.setV(arr.getShort(i));
				}
			
			};
		
		if (netcdfType.equals("int") || netcdfType.equals("enum4"))
			return new Procedure3<Array, Integer, SignedInt32Member>() {
			
				@Override
				public void call(Array arr, Integer i, SignedInt32Member out) {
					out.setV(arr.getInt(i));
				}
			
			};
		
		if (netcdfType.equals("uint"))
			return new Procedure3<Array, Integer, UnsignedInt32Member>() {
			
			@Override
			public void call(Array arr, Integer i, UnsignedInt32Member out) {
				out.setV(arr.getInt(i));
			}
		
		};
		
		if (netcdfType.equals("long"))
			return new Procedure3<Array, Integer, SignedInt64Member>() {
			
				@Override
				public void call(Array arr, Integer i, SignedInt64Member out) {
					out.setV(arr.getLong(i));
				}
			
			};
		
		if (netcdfType.equals("ulong"))
			return new Procedure3<Array, Integer, UnsignedInt64Member>() {
			
				@Override
				public void call(Array arr, Integer i, UnsignedInt64Member out) {
					out.setV(arr.getLong(i));
				}
			
			};
		
		if (netcdfType.equals("float"))
			return new Procedure3<Array, Integer, Float32Member>() {
			
				@Override
				public void call(Array arr, Integer i, Float32Member out) {
					out.setV(arr.getFloat(i));
				}
			
			};
		
		if (netcdfType.equals("double"))
			return new Procedure3<Array, Integer, Float64Member>() {
			
				@Override
				public void call(Array arr, Integer i, Float64Member out) {
					out.setV(arr.getDouble(i));
				}
			
			};

		System.out.println("No algebra can be found for "+netcdfType);

		return null;
	}

	private static void importValues(Algebra<?,?> algebra, Variable var, Procedure3<Array,Integer,Object> converter, DimensionedDataSource<Object> dataSource) {

		Object val = algebra.construct();

		Array array = null;

		// since I munge dims elsewhere these two values can differ
		
		//int rank = var.getRank();
		int rank = dataSource.numDimensions();
		
		if (rank < 0) {
			throw new IllegalArgumentException("unsupported rank in variable.");
		}
		else if (rank == 0) {
			throw new IllegalArgumentException("unexpected code fall through case");
		}
		else if (rank == 1) {
			

			try {
				array = var.read();
			} catch (IOException ex) {
				System.out.println("cannot read data from the variable");
				return;
			}

			OneDView<Object> vw = new OneDView<>(dataSource);
			for (long i = 0; i < array.getSize(); i++) {
				converter.call(array, (int) i, val);
				vw.set(i, val);
			}
		}
		else if (rank == 2) {
			
			// TODO: only call read() once?
			
			try {
				array = var.read();
			} catch (IOException ex) {
				System.out.println("cannot read data from the variable");
				return;
			}

			TwoDView<Object> vw = new TwoDView<>(dataSource);
			
			for (long y = 0; y < vw.d1(); y++) {
				for (long x = 0; x < vw.d0(); x++) {
					long ny = x;
					long nx = vw.d1() - 1 - y;
					int i = (int) (nx * vw.d0() + ny);
					converter.call(array, i, val);
					vw.set(x, y, val);
				}				
			}
		}
		else {  // rank is 3 or more

			PlaneView<Object> planes = new PlaneView<Object>(dataSource, 0, 1);

			// debugging with one dataset implies that it loads the entire dataset
			//   into ram even if it comprises 10 planes. Maybe this is not always
			//   true. Must investigate.
			try {
				array = var.read();
			} catch (IOException ex) {
				System.out.println("cannot read data from the variable");
				return;
			}
			//System.out.println("read "+array.getSize()+" elements");
			//System.out.println("  and plane dims = "+planes.d0()+" x "+planes.d1());
			
			long[] higherDims = new long[dataSource.numDimensions() - 2];
			for (int i = 0; i < higherDims.length; i++) {
				higherDims[i] = dataSource.dimension(2 + i);
			}
			
			SamplingIterator<IntegerIndex> iter = GridIterator.compute(higherDims);
			IntegerIndex idx = new IntegerIndex(higherDims.length);
			long offsetInArray = 0;
			while (iter.hasNext()) {
				
				iter.next(idx);
				
				// I believe the planes could easily be out of order due to diff
				// coord systems. Maybe not for 3d case. But 4d and higher?
				// My plane position iteration order might be totally different.
				
				for (int i = 0; i < idx.numDimensions(); i++ ) {
					planes.setPositionValue(i, idx.get(i));
				}
				
				for (long y = 0; y < planes.d1(); y++) {
					long nx = planes.d1() - 1 - y;
					for (long x = 0; x < planes.d0(); x++) {
						long ny = x;
						int i = (int) (nx * planes.d0() + ny);
						// TODO : this cast on offset+i is troubling
						// Why do array get() routines use ints and array size()
						// routines use longs?
						if ((offsetInArray + i) >= Integer.MAX_VALUE)
							throw new IllegalArgumentException("Num dims >= 3 case: index overflow!");
						converter.call(array, (int) (offsetInArray + i), val);
						planes.set(x, y, val);
					}
				}
				
				offsetInArray += (planes.d0() * planes.d1());
			}
		}
	}
}
