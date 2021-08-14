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
import java.util.Arrays;
import java.util.List;

import nom.bdezonia.zorbage.algebra.Algebra;
import nom.bdezonia.zorbage.algebra.Allocatable;
import nom.bdezonia.zorbage.algebra.G;
import nom.bdezonia.zorbage.misc.DataBundle;
import nom.bdezonia.zorbage.data.DimensionedDataSource;
import nom.bdezonia.zorbage.data.DimensionedStorage;
import nom.bdezonia.zorbage.dataview.OneDView;
import nom.bdezonia.zorbage.dataview.TwoDView;
import nom.bdezonia.zorbage.procedure.Procedure3;
import nom.bdezonia.zorbage.tuple.Tuple2;
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
	
	@SuppressWarnings({"unchecked", "rawtypes"})
	private static Tuple2<Algebra<?,?>, DimensionedDataSource<?>> readVar(Variable var, String filename) {
		String name = var.getNameAndDimensions();
		System.out.println("Var "+name);
		System.out.println("  shape " + Arrays.toString(var.getShape()));
		System.out.println("  dims  " + Arrays.toString(var.getDimensions().toArray()));
		System.out.println("  data type " + var.getDataType());
		System.out.println("  rank " + var.getRank());
		System.out.println("  size " + var.getSize());
		System.out.println("  units " + var.getUnitsString());

		int rank = var.getRank();
		
		long[] dims = new long[var.getRank()];
		for (int i = 0; i < dims.length; i++) {
			dims[i] = var.getDimension(i).getLength();
		}
		
		//System.out.println("Original DIMs = "+Arrays.toString(dims));
		
		// now fix dims in various ways to undo some netcdf weirdness
		
		// never allocate a rank 0 DS, treats a ssingle number rank 1 list of 1 element
		if (rank == 0)
			dims = new long[] {1};
		
		//System.out.println("Rank 0 to 1: DIMs = "+Arrays.toString(dims));
		
		// reverse dims because coord systems differ

		long[] tmpDims = dims.clone();
		for (int i = 0; i < dims.length; i++) {
			tmpDims[dims.length-1-i] = dims[i];
		}
		dims = tmpDims;
		
		//System.out.println("Reverse: DIMs = "+Arrays.toString(dims));
		
		// remove coords with size of 1 when its position is beyond x or y axes
		
		tmpDims = dims.clone();
		int numToKill = 0;
		for (int i = 2; i < dims.length; i++) {
			if (dims[i] == 1)
				numToKill++;
		}
		if (numToKill > 0) {
			tmpDims = new long[dims.length - numToKill];
			int valid = 0;
			if (dims.length > 0) {
				tmpDims[0] = dims[0];
				valid++;
			}
			if (dims.length > 1) {
				tmpDims[1] = dims[1];
				valid++;
			}
			for (int i = 2; i < dims.length; i++) {
				if (dims[i] != 1)
					tmpDims[valid++] = dims[i];
			}
		}
		dims = tmpDims;

		//System.out.println("Remove size 1: DIMs = "+Arrays.toString(dims));
		
		String dataType = var.getDataType().toString();
		
		Algebra<?,Allocatable> algebra = zorbageAlgebra(dataType);
		
		if (algebra == null) {
			
			System.out.println("Cannot determine how to import "+dataType);
			
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
		
		return new Tuple2<>(algebra, dataSource);
	}

	@SuppressWarnings("unchecked")
	private static <T extends Algebra<T,U>, U extends Allocatable<U>>
		T zorbageAlgebra(String netcdfType)
	{
		
		if (netcdfType.equals("char") || netcdfType.equals("String"))
			return (T) G.FSTRING;
		
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

		System.out.println("no algebra can be found for "+netcdfType);
		
		return null;
	}

	private static Procedure3<Array,Integer,?> converter(String netcdfType) {
		
		if (netcdfType.equals("char") || netcdfType.equals("String"))
			return new Procedure3<Array, Integer, FixedStringMember>() {
			
				@Override
				public void call(Array arr, Integer i, FixedStringMember out) {
					out.setV("" + arr.getChar(i));
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

		System.out.println("no algebra can be found for "+netcdfType);

		return null;
	}

	private static void importValues(Algebra<?,?> algebra, Variable var, Procedure3<Array,Integer,Object> converter, DimensionedDataSource<Object> dataSource) {

		System.out.println("VAR RANK "+var.getRank());
		System.out.println("DS NDIMS "+dataSource.numDimensions());
		
		Array array = null;

		// since I munge dims elsewhere these can differ
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
			Object val = algebra.construct();
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
			Object val = algebra.construct();
			int nRows = var.getDimension(0).getLength();
			int nCols = var.getDimension(1).getLength();
			
			System.out.println("NROWS = "+nRows);
			System.out.println("NCOLS = "+nCols);

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

			System.out.println("MUST DO A PLANE VIEW APPROACH");
			
			// TODO: do a PlaneView approach
			//   Do I need to do array.read() a bunch of times? once per plane?
			//   what about n-d??
		}

	}
	
	
	/*

	private static List<DimensionedDataSource<UnsignedInt1Member>> readBools(NetcdfFile file) {
		Procedure3<Array, Integer, UnsignedInt1Member> assignProc =
				new Procedure3<Array, Integer, UnsignedInt1Member>()
		{
			@Override
			public void call(Array arr, Integer index, UnsignedInt1Member output) {
				boolean v = arr.getBoolean(index);
				output.setV(v ? 1 : 0);
			}
		};
		return readValues(file, new String[] {"boolean"}, G.UINT1.construct(), assignProc);
	}

	private static List<DimensionedDataSource<FixedStringMember>> readStrings(NetcdfFile file) throws IOException {
		// Return all the "char" bands as metatdata
		// Also (for now) try a simple attempt (might be broken) at returning "String" metadata
		// notes about the unsupported types that weren't loaded like sequences or string or
		// structure or opaque or object??
		List<DimensionedDataSource<FixedStringMember>> datasets = new ArrayList<>();
		List<Variable> vars = file.getVariables();
		for (Variable var : vars) {
			String dataType = var.getDataType().toString();
			String name = var.getNameAndDimensions();
			if (dataType.equals("char") || dataType.equals("String"))
			{
				String str = var.read().toString();
				FixedStringMember value = new FixedStringMember(str);
				DimensionedDataSource<FixedStringMember> ds = DimensionedStorage.allocate(value, new long[] {1});
				ds.rawData().set(0, value);
				datasets.add(ds);
			}
			else if (dataType.equals("boolean") ||
					dataType.equals("byte") ||
					dataType.equals("ubyte") ||
					dataType.equals("short") ||
					dataType.equals("ushort") ||
					dataType.equals("int") ||
					dataType.equals("uint") ||
					dataType.equals("long") ||
					dataType.equals("ulong") ||
					dataType.equals("float") ||
					dataType.equals("double") ||
					dataType.equals("enum1") ||
					dataType.equals("enum2") ||
					dataType.equals("enum4"))
			{
				// do nothing : ignore : these will be loaded elsewhere
			}
			else if (dataType.equals("Sequence") ||
						dataType.equals("Structure") ||
						dataType.equals("opaque") ||
						dataType.equals("object"))
			{
				// note that some types our reader can't load yet
				String str = name + ": Unsupported data set of type " + dataType + ": not loaded";
				FixedStringMember value = new FixedStringMember(str);
				DimensionedDataSource<FixedStringMember> ds = DimensionedStorage.allocate(value, new long[] {1});
				ds.rawData().set(0, value);
				datasets.add(ds);
			}
			else
			{
				// note that some types might be newer than the netcdf code we are using supports
				String str = name + ": unknown data set of type " + dataType + ": ignored";
				FixedStringMember value = new FixedStringMember(str);
				DimensionedDataSource<FixedStringMember> ds = DimensionedStorage.allocate(value, new long[] {1});
				ds.rawData().set(0, value);
				datasets.add(ds);
			}
		}
		return datasets;
	}
	
	private static <U extends Allocatable<U>>
		List<DimensionedDataSource<U>> readValues(NetcdfFile file, String[] types, U val, Procedure3<Array,Integer,U> assignProc)
	{
		List<Info> bandGroups = bandInfo(file, types);
		System.out.println("One call to readValues() and numBandGroups = "+bandGroups.size());
		List<DimensionedDataSource<U>> datasets = new ArrayList<>();
		for (Info info : bandGroups) {
			System.out.println("  num bands in one group = " + info.bandNums.size());
			System.out.println("  size of each band in one group = " + info.size);
			System.out.println("  band nums in this group = " + Arrays.toString(info.bandNums.toArray()));			
			DimensionedDataSource<U> ds = makeDataset(info, file, val);
			System.out.println("Dataset made with dims");
			for (int i = 0; i < ds.numDimensions(); i++) {
				System.out.println("  " + i + " " + ds.dimension(i));
			}
			ds.setName(file.getTitle());
			ds.setSource(file.getLocation());
			PlaneView<U> planes = new PlaneView<>(ds, 0, 1);
			
			int numPlaneDims = ds.numDimensions() - 2;
			if (numPlaneDims < 0) numPlaneDims = 0;
			
			Variable var;
			if (numPlaneDims == 0) {
				int band = info.bandNums.get(0);
				var = file.getVariables().get(band);
				ds.metadata().putString("band-"+0+"-location", var.getDatasetLocation());
				ds.metadata().putString("band-"+0+"-description", var.getDescription());
				ds.metadata().putString("band-"+0+"-dimensions", var.getDimensionsString());
				ds.metadata().putString("band-"+0+"-file-type-id", var.getFileTypeId());
				ds.metadata().putString("band-"+0+"-name-and-dimensions", var.getNameAndDimensions());
				ds.metadata().putString("band-"+0+"-units", var.getUnitsString());
				Array arr = null;
				try {
					arr = var.read();
				} catch (IOException e) {
					throw new IllegalArgumentException("could not read data array for band number "+band);
				}
				// now iterate the plane of pixels and fill it
				for (long y = 0; y < planes.d1(); y++) {
					long transformedY = planes.d1() - 1 - y;
					for (long x = 0; x < planes.d0(); x++) {
						long transformedX = x;
						long p = (planes.d0() * transformedY) + transformedX;
						if (p >= Integer.MAX_VALUE)
							throw new IllegalArgumentException("netcdf dims are larger than expected : 1");
						assignProc.call(arr, (int) (p), val);
						planes.set(x, y, val);
					}
				}
			}
			else { // we have multiple planes
				
				long[] planeDims = new long[numPlaneDims];
				for (int p = 0; p < numPlaneDims; p++) {
					planeDims[p] = ds.dimension(p+2);
				}
				
				System.out.println("PLANEDIMS = "+Arrays.toString(planeDims));		
				
				SamplingIterator<IntegerIndex> iter = GridIterator.compute(planeDims);
				
				IntegerIndex planeIdx = new IntegerIndex(planeDims.length);

				int band = info.bandNums.get(0);
				
				var = file.getVariables().get(band);
				
				System.out.println("dims of variable that holds bands" + var.getDimensions());

				// iterate a plane at a time

				int bandNum = 0;
				while (iter.hasNext()) {
					
					// find the next plane 
					iter.next(planeIdx);
				
					System.out.println("PLANEIDX = "+planeIdx.toString());		

					var = file.getVariables().get(bandNum);
					
					ds.metadata().putString("band-"+bandNum+"-location", var.getDatasetLocation());
					ds.metadata().putString("band-"+bandNum+"-description", var.getDescription());
					ds.metadata().putString("band-"+bandNum+"-dimensions", var.getDimensionsString());
					ds.metadata().putString("band-"+bandNum+"-file-type-id", var.getFileTypeId());
					ds.metadata().putString("band-"+bandNum+"-name-and-dimensions", var.getNameAndDimensions());
					ds.metadata().putString("band-"+bandNum+"-units", var.getUnitsString());

					bandNum++;
					
					Array arr = null;
					try {
						System.out.println("  before read and var says it's dims are " + var.getDimensions());
						arr = var.read();
						System.out.println("  read " + arr.getSize() + " elements");
						System.out.println("  after read and arr says it's shape is " + Arrays.toString(arr.getShape()));
					} catch (IOException e) {
						throw new IllegalArgumentException("could not read data array for band number "+band);
					}

					// set our plane's position based upon this index
					for (int d = 0; d < planeDims.length; d++) {
						planes.setPositionValue(d, planeIdx.get(d));
					}

					// now iterate the plane of pixels and fill it
					for (long y = 0; y < planes.d1(); y++) {
						long transformedY = planes.d1() - 1 - y;
						for (long x = 0; x < planes.d0(); x++) {
							long transformedX = x;
							long p = (planes.d0() * transformedY) + transformedX;
							if (p >= Integer.MAX_VALUE)
								throw new IllegalArgumentException("netcdf dims are larger than expected : 1");
							assignProc.call(arr, (int) (p), val);
							planes.set(x, y, val);
						}
					}
				}
			}
			datasets.add(ds);
		}
		return datasets;
	}
	
	private static List<DimensionedDataSource<UnsignedInt1Member>> readBools(NetcdfFile file) {
		Procedure3<Array, Integer, UnsignedInt1Member> assignProc =
				new Procedure3<Array, Integer, UnsignedInt1Member>()
		{
			@Override
			public void call(Array arr, Integer index, UnsignedInt1Member output) {
				boolean v = arr.getBoolean(index);
				output.setV(v ? 1 : 0);
			}
		};
		return readValues(file, new String[] {"boolean"}, G.UINT1.construct(), assignProc);
	}
	
	private static List<DimensionedDataSource<SignedInt8Member>> readBytes(NetcdfFile file) {
		Procedure3<Array, Integer, SignedInt8Member> assignProc =
				new Procedure3<Array, Integer, SignedInt8Member>()
		{
			@Override
			public void call(Array arr, Integer index, SignedInt8Member output) {
				byte v = arr.getByte(index);
				output.setV(v);
			}
		};
		return readValues(file, new String[] {"byte","enum1"}, G.INT8.construct(), assignProc);
	}
	
	private static List<DimensionedDataSource<UnsignedInt8Member>> readUBytes(NetcdfFile file) {
		Procedure3<Array, Integer, UnsignedInt8Member> assignProc =
				new Procedure3<Array, Integer, UnsignedInt8Member>()
		{
			@Override
			public void call(Array arr, Integer index, UnsignedInt8Member output) {
				byte v = arr.getByte(index);
				output.setV(v);
			}
		};
		return readValues(file, new String[] {"ubyte"}, G.UINT8.construct(), assignProc);
	}
	
	private static List<DimensionedDataSource<SignedInt16Member>> readShorts(NetcdfFile file) {
		Procedure3<Array, Integer, SignedInt16Member> assignProc =
				new Procedure3<Array, Integer, SignedInt16Member>()
		{
			@Override
			public void call(Array arr, Integer index, SignedInt16Member output) {
				short v = arr.getShort(index);
				output.setV(v);
			}
		};
		return readValues(file, new String[] {"short","enum2"}, G.INT16.construct(), assignProc);
	}
	
	private static List<DimensionedDataSource<UnsignedInt16Member>> readUShorts(NetcdfFile file) {
		Procedure3<Array, Integer, UnsignedInt16Member> assignProc =
				new Procedure3<Array, Integer, UnsignedInt16Member>()
		{
			@Override
			public void call(Array arr, Integer index, UnsignedInt16Member output) {
				short v = arr.getShort(index);
				output.setV(v);
			}
		};
		return readValues(file, new String[] {"ushort"}, G.UINT16.construct(), assignProc);
	}
	
	private static List<DimensionedDataSource<SignedInt32Member>> readInts(NetcdfFile file) {
		Procedure3<Array, Integer, SignedInt32Member> assignProc =
				new Procedure3<Array, Integer, SignedInt32Member>()
		{
			@Override
			public void call(Array arr, Integer index, SignedInt32Member output) {
				int v = arr.getInt(index);
				output.setV(v);
			}
		};
		return readValues(file, new String[] {"int","enum4"}, G.INT32.construct(), assignProc);
	}
	
	private static List<DimensionedDataSource<UnsignedInt32Member>> readUInts(NetcdfFile file) {
		Procedure3<Array, Integer, UnsignedInt32Member> assignProc =
				new Procedure3<Array, Integer, UnsignedInt32Member>()
		{
			@Override
			public void call(Array arr, Integer index, UnsignedInt32Member output) {
				int v = arr.getInt(index);
				output.setV(v);
			}
		};
		return readValues(file, new String[] {"uint"}, G.UINT32.construct(), assignProc);
	}
	
	private static List<DimensionedDataSource<SignedInt64Member>> readLongs(NetcdfFile file) {
		Procedure3<Array, Integer, SignedInt64Member> assignProc =
				new Procedure3<Array, Integer, SignedInt64Member>()
		{
			@Override
			public void call(Array arr, Integer index, SignedInt64Member output) {
				long v = arr.getLong(index);
				output.setV(v);
			}
		};
		return readValues(file, new String[] {"long"}, G.INT64.construct(), assignProc);
	}
	
	private static List<DimensionedDataSource<UnsignedInt64Member>> readULongs(NetcdfFile file) {
		Procedure3<Array, Integer, UnsignedInt64Member> assignProc =
				new Procedure3<Array, Integer, UnsignedInt64Member>()
		{
			@Override
			public void call(Array arr, Integer index, UnsignedInt64Member output) {
				long v = arr.getLong(index);
				output.setV(v);
			}
		};
		return readValues(file, new String[] {"ulong"}, G.UINT64.construct(), assignProc);
	}
	
	private static List<DimensionedDataSource<Float32Member>> readFloats(NetcdfFile file) {
		Procedure3<Array, Integer, Float32Member> assignProc =
				new Procedure3<Array, Integer, Float32Member>()
		{
			@Override
			public void call(Array arr, Integer index, Float32Member output) {
				float v = arr.getFloat(index);
				output.setV(v);
			}
		};
		return readValues(file, new String[] {"float"}, G.FLT.construct(), assignProc);
	}
	
	private static List<DimensionedDataSource<Float64Member>> readDoubles(NetcdfFile file) {
		Procedure3<Array, Integer, Float64Member> assignProc =
				new Procedure3<Array, Integer, Float64Member>()
		{
			@Override
			public void call(Array arr, Integer index, Float64Member output) {
				double v = arr.getDouble(index);
				output.setV(v);
			}
		};
		return readValues(file, new String[] {"double"}, G.DBL.construct(), assignProc);
	}

	private static class Info {
		long size = 0;
		List<Integer> bandNums = new ArrayList<>();
	}
	
	private static boolean contains(String[] keys, String key) {
		for (String k : keys) {
			if (key.equals(k))
				return true;
		}
		return false;
	}
	
	private static List<Info> bandInfo(NetcdfFile file, String[] types) {
		List<Info> infos = new ArrayList<>();
		List<Variable> vars = file.getVariables();
		for (int band = 0; band < vars.size(); band++) {
			Variable var = vars.get(band);
			String type = var.getDataType().toString();
			if (contains(types, type)) {
				Info found = null;
				for (Info info : infos) {
					if (info.size == var.getSize()) {
						found = info;
						break;
					}
				}
				if (found == null) {
					Info newOne = new Info();
					newOne.size = var.getSize();
					newOne.bandNums.add(band);
					infos.add(newOne);
				}
				else {
					found.bandNums.add(band);
				}
			}
		}
		return infos;
	}
	
	private static <U extends Allocatable<U>> DimensionedDataSource<U>
		makeDataset(Info info, NetcdfFile file, U type)
	{
		// set the dims to the file's specified dims
		
		List<Dimension> dims = file.getDimensions();
		long[] dimsStep1 = new long[dims.size()];
		for (int i = 0; i < dims.size(); i++) {
			dimsStep1[i] = dims.get(i).getLength();
		}
		
		System.out.println("dims from file = " + Arrays.toString(dimsStep1));
		
		// see if the bands of the set match this size
		
		long numElems = LongUtils.numElements(dimsStep1);
		List<Variable> bands = file.getVariables();
		int templateBand = -1;
		for (int i = 0; i < info.bandNums.size(); i++) {
			if (bands.get(info.bandNums.get(i)).getSize() != numElems) {
				templateBand = info.bandNums.get(i);
				break;
			}
		}
		
		// set the dims from a representative band if needed
		
		long[] dimsStep2 = dimsStep1;
		if (templateBand != -1) {
			Variable band = bands.get(templateBand);
			List<Dimension> bandDims = band.getDimensions();
			List<Integer> newDims = new ArrayList<>();
			for (Dimension d : bandDims) {
				newDims.add(d.getLength());
			}
			dimsStep2 = new long[newDims.size()];
			for (int i = 0; i < newDims.size(); i++) {
				dimsStep2[i] = newDims.get(i);
			}
		}
		
		System.out.println("dims from band = " + Arrays.toString(dimsStep2));
		
		// reverse the dims since netcdf does not match zorbage's conventions

		long[] dimsStep3 = new long[dimsStep2.length];
		for (int i = 0, last = dimsStep2.length-1; i < dimsStep2.length; i++, last--) {
			dimsStep3[i] = dimsStep2[last];
		}
		
		System.out.println("reversed dims to match our conventions = " + Arrays.toString(dimsStep3));

		
		// remove the dimensions of size 1
		
		List<Long> nonOnes = new ArrayList<>();
		for (int i = 0; i < dimsStep3.length; i++) {
			if (dimsStep3[i] > 1)
				nonOnes.add(dimsStep3[i]);
		}
		long[] dimsStep4 = new long[nonOnes.size()];
		for (int i = 0; i < nonOnes.size(); i++) {
			dimsStep4[i] = nonOnes.get(i);
		}
		
		System.out.println("dims with no trivial dims = " + Arrays.toString(dimsStep4));

		// add the band count as a dim if necessary

		long[] dimsStep5 = dimsStep4;

		if (info.bandNums.size() > 1) {
			long[] tmp = new long[dimsStep5.length + 1];
			for (int i = 0; i < dimsStep5.length; i++) {
				tmp[i] = dimsStep5[i];
			}
			tmp[dimsStep5.length] = info.bandNums.size();
			dimsStep5 = tmp;
		}

		System.out.println("dims with band count added = " + Arrays.toString(dimsStep5));

		// make sure we've not made empty dims
		
		if (dimsStep5.length == 0) {
			dimsStep5 = new long[] {1};
			System.out.println("dims that avoid catastrophe = " + Arrays.toString(dimsStep5));
		}

		return DimensionedStorage.allocate(type, dimsStep5);
	}

	 */

}
