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
import java.util.ArrayList;
import java.util.List;

import nom.bdezonia.zorbage.algebra.Allocatable;
import nom.bdezonia.zorbage.algebra.G;
import nom.bdezonia.zorbage.algorithm.GridIterator;
import nom.bdezonia.zorbage.misc.DataBundle;
import nom.bdezonia.zorbage.misc.LongUtils;
import nom.bdezonia.zorbage.data.DimensionedDataSource;
import nom.bdezonia.zorbage.data.DimensionedStorage;
import nom.bdezonia.zorbage.procedure.Procedure3;
import nom.bdezonia.zorbage.sampling.IntegerIndex;
import nom.bdezonia.zorbage.sampling.SamplingIterator;
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
import ucar.nc2.Dimension;
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
	public static DataBundle loadAllDatasets(String filename) {
		DataBundle bundle = new DataBundle();
		try {
			NetcdfFile file = NetcdfFiles.open(filename);
			bundle.uint1s = readBools(file);
			bundle.int8s = readBytes(file);
			bundle.uint8s = readUBytes(file);
			bundle.int16s = readShorts(file);
			bundle.uint16s = readUShorts(file);
			bundle.int32s = readInts(file);
			bundle.uint32s = readUInts(file);
			bundle.int64s = readLongs(file);
			bundle.uint64s = readULongs(file);
			bundle.flts = readFloats(file);
			bundle.dbls = readDoubles(file);
			bundle.fstrs = readStrings(file);
		} catch (IOException e) {
			System.out.println("Exception occured : " + e);
		}
		return bundle;
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
		List<DimensionedDataSource<U>> datasets = new ArrayList<>();
		for (Info info : bandGroups) {
			DimensionedDataSource<U> ds = makeDataset(info, file, val);
			ds.setName(file.getTitle());
			ds.setSource(file.getLocation());
			for (int i = 0; i < info.bandNums.size(); i++) {
				int band = info.bandNums.get(i);
				Variable var = file.getVariables().get(band);
				ds.metadata().put("band-"+i+"-location", var.getDatasetLocation());
				ds.metadata().put("band-"+i+"-description", var.getDescription());
				ds.metadata().put("band-"+i+"-dimensions", var.getDimensionsString());
				ds.metadata().put("band-"+i+"-file-type-id", var.getFileTypeId());
				ds.metadata().put("band-"+i+"-name-and-dimensions", var.getNameAndDimensions());
				ds.metadata().put("band-"+i+"-units", var.getUnitsString());
				Array arr = null;
				try {
					arr = var.read();
				} catch (IOException e) {
					throw new IllegalArgumentException("could not read data array for band number "+band);
				}
				long[] minPt = new long[ds.numDimensions()];
				long[] maxPt = new long[ds.numDimensions()];
				for (int d = 0; d < ds.numDimensions(); d++) {
					maxPt[d] = ds.dimension(d) - 1;
				}
				// set the plane to the one band we are reading (only if ds data is multichannel)
				if (info.bandNums.size() != 1) {
					minPt[0] = i;
					maxPt[0] = i;
				}
				SamplingIterator<IntegerIndex> iter = GridIterator.compute(minPt, maxPt);
				IntegerIndex index = new IntegerIndex(ds.numDimensions());
				int p = 0;
				long xBound = (maxPt[0] + 1);
				long yBound = (maxPt.length < 2) ? 1 : (maxPt[1] + 1);
				for (long y = 0; y < yBound; y++) {
					for (long x = 0; x < xBound; x++) {
						iter.next(index);
						p = (int) ((yBound - 1 - y) * (xBound) + x);
						assignProc.call(arr, p++, val);
						ds.set(index, val);
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
		
		// reverse the dims since netcdf does not match zorbage's conventions

		long[] dimsStep3 = new long[dimsStep2.length];
		for (int i = 0, last = dimsStep2.length-1; i < dimsStep2.length; i++, last--) {
			dimsStep3[i] = dimsStep2[last];
		}
		
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
		
		// add the band count as a dim if necessary

		long[] dimsStep5 = dimsStep4;
		if (info.bandNums.size() > 1) {
			long[] tmp = new long[dimsStep5.length + 1];
			tmp[0] = info.bandNums.size();
			for (int i = 1; i < tmp.length; i++) {
				tmp[i] = dimsStep5[i-1];
			}
			dimsStep5 = tmp;
		}

		// make sure we've not made empty dims
		
		if (dimsStep5.length == 0)
			dimsStep5 = new long[] {1};

		return DimensionedStorage.allocate(type, dimsStep5);
	}
	
}
