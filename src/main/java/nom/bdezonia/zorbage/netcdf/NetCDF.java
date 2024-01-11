/*
 * zorbage-netcdf: code for using the NetCDF data file library to open files into zorbage data structures for further processing
 *
 * Copyright (C) 2020-2022 Barry DeZonia
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

import nom.bdezonia.zorbage.algebra.Addition;
import nom.bdezonia.zorbage.algebra.Algebra;
import nom.bdezonia.zorbage.algebra.Allocatable;
import nom.bdezonia.zorbage.algebra.G;
import nom.bdezonia.zorbage.algorithm.GridIterator;
import nom.bdezonia.zorbage.algorithm.ScaleByDouble;
import nom.bdezonia.zorbage.algorithm.TransformWithConstant;
import nom.bdezonia.zorbage.misc.DataBundle;
import nom.bdezonia.zorbage.misc.DataSourceUtils;
import nom.bdezonia.zorbage.data.DimensionedDataSource;
import nom.bdezonia.zorbage.data.DimensionedStorage;
import nom.bdezonia.zorbage.data.NdData;
import nom.bdezonia.zorbage.datasource.IndexedDataSource;
import nom.bdezonia.zorbage.procedure.Procedure2;
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
import ucar.nc2.Attribute;
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
	public static
	 		<T extends Algebra<T,U> & Addition<U> & nom.bdezonia.zorbage.algebra.ScaleByDouble<U>, U>
		DataBundle loadAllDatasets(String filename)
	{
		DataBundle bundle = new DataBundle();
		try {
			NetcdfFile file = NetcdfFiles.open(filename);
			
			List<Variable> vars = file.getVariables();

			for (Variable var : vars) {
			
				Tuple2<T, DimensionedDataSource<U>> dataSource = readVar(var, filename);
				
				if (dataSource == null)
					continue;
				
				merge(bundle, dataSource, var.getDataType().toString());
			}
		}
		catch (IOException e) {
			System.out.println("Exception occurred : " + e);
		}
		return bundle;
	}
	
	@SuppressWarnings("unchecked")
	private static
	 	<T extends Algebra<T,U> & nom.bdezonia.zorbage.algebra.ScaleByDouble<U>, U>
		void merge(DataBundle bundle, Tuple2<T, DimensionedDataSource<U>> dataSource, String dataType)
	{
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
			System.out.println("Ignoring unknown data type : " + dataType);
		}
	}
	
	// BDZ 8-15-21
	// Note re: using deprecated NetCDF code: I looked in the latest code on their github site and
	//   the getShortName() is not deprecated and is a key part of Variable/Dimension designs.
	
	@SuppressWarnings({"unchecked", "rawtypes", "deprecation"})
	private static <T extends Algebra<T,U> & Addition<U> & nom.bdezonia.zorbage.algebra.ScaleByDouble<U>, U>
		Tuple2<T, DimensionedDataSource<U>> readVar(Variable var, String filename)
	{
		// TODO try as I might I cannot find any info about axis calibrations/scales/offsets.
		// I did find a web page that says some people encode annotations as "scale_factor"
		// and "add_offset" but I'm not finding them in at least some of my data. Ask in
		// community how to find this info.

		double varScale = 1;
		double varOffset = 0;
		
		boolean varHasScale = false;
		boolean varHasOffset = false;
		
		Attribute att;
		att = var.attributes().findAttribute("scale_factor");
		if (att != null) {
			Number value = att.getNumericValue();
			if (value != null) {
				varHasScale = true;
				varScale = value.doubleValue();
			}
		}
		att = var.attributes().findAttribute("add_offset");
		if (att != null) {
			Number value = att.getNumericValue();
			if (value != null) {
				varHasOffset = true;
				varOffset = value.doubleValue();
			}
		}
		
		int rank = var.getRank();
		
		long[] dims = new long[rank];
		String[] axisLabels = new String[rank];
		for (int i = 0; i < dims.length; i++) {
			dims[i] = var.getDimension(i).getLength();
			axisLabels[i] = var.getDimension(i).getShortName();
		}
		
		// now fix dims and units in various ways to undo some netcdf weirdness
		
		// never allocate a rank 0 DS, treats as single number: a rank 1 list of 1 element
		if (rank == 0) {
			dims = new long[] {1};
			axisLabels = new String[] {"value"};
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
		
		String dataType = var.getDataType().toString();
		
		Algebra<?,Allocatable> algebra = zorbageAlgebra(dataType);
		
		if (algebra == null) {
			
			System.out.println("Cannot determine how to import "+dataType+". Ignoring data source "+var.getShortName()+".");
			
			return null;
		}		
		
		Allocatable type = algebra.construct();
		
		// for fixed strings if you do not allocate a max size then
		//   every string.setV() will do nothing.
		
		if (type instanceof FixedStringMember) {
			
			type = new FixedStringMember(256);
		}
		
		Procedure2<Array,Object> converterProc = (Procedure2<Array,Object>) converter(var.getDataType().toString());
		
		DimensionedDataSource<Object> dataSource = DimensionedStorage.allocate(type, dims);

		importValues(algebra, type, var, converterProc, dataSource);

		long[] compressedDims = normalizeDims(dims);
		
		DimensionedDataSource<U> finalDS = (DimensionedDataSource<U>) new NdData<>(compressedDims, dataSource.rawData());
		
		finalDS.setName(var.getNameAndDimensions());
		
		finalDS.setSource(filename);

		finalDS.setValueUnit(var.getUnitsString());

		// these first two dims can have 1's in them
		
		if (finalDS.numDimensions() > 0) {
			finalDS.setAxisType(0, axisLabels[0]);
		}
			
		if (finalDS.numDimensions() > 1) {
			finalDS.setAxisType(1, axisLabels[1]);
		}
		
		// any other dims == 1 in origDs have to be accounted for;

		int count = 2;
		for (int i = 2; i < axisLabels.length; i++) {
			if (dataSource.dimension(i) == 1)
				continue;
			finalDS.setAxisType(count, axisLabels[i]);
			count++;
		}
		
		// Finally capture any special scaling if necessary
		//   In practice maybe people scale Short backed files into Doubles this way
		//   thus saving storage space. Nifti and/or Ecat do similar things.
		
		// NOTE: my current code does not transmute type (for instance from short
		//   to double).
		
		if (varHasScale) {
			
			ScaleByDouble.compute((T) algebra,
									varScale,
									(IndexedDataSource<U>) finalDS.rawData(),
									(IndexedDataSource<U>) finalDS.rawData());
		}
		
		if (varHasOffset) {

			TransformWithConstant.compute((T) algebra,
											((T)algebra).add(),
											(IndexedDataSource<U>) finalDS.rawData(),
											(U) algebra.construct(""+varOffset),
											(IndexedDataSource<U>) finalDS.rawData());
		}

		return new Tuple2<T,DimensionedDataSource<U>>((T) algebra, finalDS);
	}

	// remove dimensions of size one when they are not x nor y
	
	private static long[] normalizeDims(long[] dims) {

		int count = 0;
		if (dims.length > 0) {
			count++;
		}
		if (dims.length > 1) {
			count++;
		}
		for (int d = 2; d < dims.length; d++) {
			if (dims[d] > 1) {
				count++;
			}
		}
		
		long[] newDims = new long[count];

		newDims = new long[count];
		int i = 0;
		for (int d = 0; d < dims.length; d++) {
			if (d == 0 || d == 1 || dims[d] > 1) {
				newDims[i++] = dims[d];
			}
		}
		
		return newDims;
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

	private static Procedure2<Array,?> converter(String netcdfType) {
		
		if (netcdfType.equals("char"))
			return new Procedure2<Array, CharMember>() {
			
				@Override
				public void call(Array arr, CharMember out) {
					out.setV(arr.nextChar());
				}
			
			};
		
		if (netcdfType.equalsIgnoreCase("String"))
			return new Procedure2<Array, FixedStringMember>() {
			
				@Override
				public void call(Array arr, FixedStringMember out) {
					out.setV(arr.toString());
				}
			
			};
			
		if (netcdfType.equals("boolean"))
			return new Procedure2<Array, UnsignedInt1Member>() {
			
				@Override
				public void call(Array arr, UnsignedInt1Member out) {
					out.setV(arr.nextBoolean() ? 1 : 0);
				}
			
			};
		
		if (netcdfType.equals("byte") || netcdfType.equals("enum1"))
			return new Procedure2<Array, SignedInt8Member>() {
			
				@Override
				public void call(Array arr, SignedInt8Member out) {
					out.setV(arr.nextByte());
				}
			
			};
		
		if (netcdfType.equals("ubyte"))
			return new Procedure2<Array, UnsignedInt8Member>() {
			
				@Override
				public void call(Array arr, UnsignedInt8Member out) {
					out.setV(arr.nextByte());
				}
			
			};
		
		if (netcdfType.equals("short") || netcdfType.equals("enum2"))
			return new Procedure2<Array, SignedInt16Member>() {
			
				@Override
				public void call(Array arr, SignedInt16Member out) {
					out.setV(arr.nextShort());
				}
			
			};
		
		if (netcdfType.equals("ushort"))
			return new Procedure2<Array, UnsignedInt16Member>() {
			
				@Override
				public void call(Array arr, UnsignedInt16Member out) {
					out.setV(arr.nextShort());
				}
			
			};
		
		if (netcdfType.equals("int") || netcdfType.equals("enum4"))
			return new Procedure2<Array, SignedInt32Member>() {
			
				@Override
				public void call(Array arr, SignedInt32Member out) {
					out.setV(arr.nextInt());
				}
			
			};
		
		if (netcdfType.equals("uint"))
			return new Procedure2<Array, UnsignedInt32Member>() {
			
			@Override
			public void call(Array arr, UnsignedInt32Member out) {
				out.setV(arr.nextInt());
			}
		
		};
		
		if (netcdfType.equals("long"))
			return new Procedure2<Array, SignedInt64Member>() {
			
				@Override
				public void call(Array arr, SignedInt64Member out) {
					out.setV(arr.nextLong());
				}
			
			};
		
		if (netcdfType.equals("ulong"))
			return new Procedure2<Array, UnsignedInt64Member>() {
			
				@Override
				public void call(Array arr, UnsignedInt64Member out) {
					out.setV(arr.nextLong());
				}
			
			};
		
		if (netcdfType.equals("float"))
			return new Procedure2<Array, Float32Member>() {
			
				@Override
				public void call(Array arr, Float32Member out) {
					out.setV(arr.nextFloat());
				}
			
			};
		
		if (netcdfType.equals("double"))
			return new Procedure2<Array, Float64Member>() {
			
				@Override
				public void call(Array arr, Float64Member out) {
					out.setV(arr.nextDouble());
				}
			
			};

		System.out.println("No algebra can be found for "+netcdfType);

		return null;
	}

	private static void importValues(Algebra<?,?> algebra, Object val, Variable var,
										Procedure2<Array,Object> converter,
										DimensionedDataSource<Object> dataSource)
	{
		// netcdf dims are stored and data is written in reverse order.
		// but to simplify code ignore that for now.
		
		long[] netCDFDims = DataSourceUtils.dimensions(dataSource);
		
		Array array = null;
		
		// iterate NetCDF space

		SamplingIterator<IntegerIndex> iter = GridIterator.compute(netCDFDims);
		IntegerIndex netCDFIdx = new IntegerIndex(netCDFDims.length);
		IntegerIndex zorbageIdx = new IntegerIndex(netCDFDims.length);
		while (iter.hasNext()) {

			if (array == null || ((!array.hasNext()) && (iter.hasNext()))) {
				try {
					array = var.read();
					// kludge: set the Array's internal iterator so it is not null
					array.hasNext();
				} catch (IOException exc) {
					System.out.println("Could not read an Array from a Variable.");
					return;
				}
			}

			// get the next value from the array and convert it into a zorbage type value
			
			converter.call(array, val);
			
			// update the netcdf coord to point to the location associated with value just read
			
			iter.next(netCDFIdx);
			
			// now xform coords from netcdf space to zorbage space

			int maxD = netCDFIdx.numDimensions() - 1;
			for (int netcdfDim = 0; netcdfDim <= maxD; netcdfDim++) {
				
				int zorbageDim = netcdfDim;
				
				long netcdfPos = netCDFIdx.get(netcdfDim);
				
				long zorbagePos;
				
				// of course NetCDF has a different Y dim convention

				if (zorbageDim == 1) {

					// flip Y
					long flippedY = netCDFDims[netcdfDim] - 1 - netcdfPos;
					
					zorbagePos = flippedY;
				}
				else {
					
					zorbagePos = netcdfPos;
				}

				zorbageIdx.set(zorbageDim, zorbagePos);
			}

			dataSource.set(zorbageIdx, val);
		}
	}
}
