package zorbage.netcdf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nom.bdezonia.zorbage.algebra.Allocatable;
import nom.bdezonia.zorbage.algebra.G;
import nom.bdezonia.zorbage.misc.LongUtils;
import nom.bdezonia.zorbage.multidim.MultiDimDataSource;
import nom.bdezonia.zorbage.multidim.MultiDimStorage;
import nom.bdezonia.zorbage.procedure.Procedure3;
import nom.bdezonia.zorbage.sampling.IntegerIndex;
import nom.bdezonia.zorbage.sampling.SamplingCartesianIntegerGrid;
import nom.bdezonia.zorbage.sampling.SamplingIterator;
import nom.bdezonia.zorbage.tuple.Tuple12;
import nom.bdezonia.zorbage.type.float32.real.Float32Member;
import nom.bdezonia.zorbage.type.float64.real.Float64Member;
import nom.bdezonia.zorbage.type.int1.UnsignedInt1Member;
import nom.bdezonia.zorbage.type.int16.SignedInt16Member;
import nom.bdezonia.zorbage.type.int16.UnsignedInt16Member;
import nom.bdezonia.zorbage.type.int32.SignedInt32Member;
import nom.bdezonia.zorbage.type.int32.UnsignedInt32Member;
import nom.bdezonia.zorbage.type.int64.SignedInt64Member;
import nom.bdezonia.zorbage.type.int64.UnsignedInt64Member;
import nom.bdezonia.zorbage.type.int8.SignedInt8Member;
import nom.bdezonia.zorbage.type.int8.UnsignedInt8Member;
import ucar.ma2.Array;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFiles;
import ucar.nc2.Variable;

public class Main
{
	public Tuple12<
		Map<String,String>,
		List<MultiDimDataSource<UnsignedInt1Member>>,
		List<MultiDimDataSource<SignedInt8Member>>,
		List<MultiDimDataSource<UnsignedInt8Member>>,
		List<MultiDimDataSource<SignedInt16Member>>,
		List<MultiDimDataSource<UnsignedInt16Member>>,
		List<MultiDimDataSource<SignedInt32Member>>,
		List<MultiDimDataSource<UnsignedInt32Member>>,
		List<MultiDimDataSource<SignedInt64Member>>,
		List<MultiDimDataSource<UnsignedInt64Member>>,
		List<MultiDimDataSource<Float32Member>>,
		List<MultiDimDataSource<Float64Member>>>
	loadDataSources(NetcdfFile file)
	throws IOException
	{
		Map<String,String> strings = readStrings(file);
		List<MultiDimDataSource<UnsignedInt1Member>> bools = readBools(file);
		List<MultiDimDataSource<SignedInt8Member>>  bytes = readBytes(file);
		List<MultiDimDataSource<UnsignedInt8Member>> ubytes = readUBytes(file);
		List<MultiDimDataSource<SignedInt16Member>> shorts = readShorts(file);
		List<MultiDimDataSource<UnsignedInt16Member>> ushorts = readUShorts(file);
		List<MultiDimDataSource<SignedInt32Member>> ints = readInts(file);
		List<MultiDimDataSource<UnsignedInt32Member>> uints = readUInts(file);
		List<MultiDimDataSource<SignedInt64Member>> longs = readLongs(file);
		List<MultiDimDataSource<UnsignedInt64Member>> ulongs = readULongs(file);
		List<MultiDimDataSource<Float32Member>> floats = readFloats(file);
		List<MultiDimDataSource<Float64Member>> doubles = readDoubles(file);
		return new Tuple12<>(strings, bools, bytes, ubytes, shorts, ushorts, ints, uints, longs, ulongs, floats, doubles);
	}
	
	private Map<String,String> readStrings(NetcdfFile file) throws IOException {
		// Return all the "char" bands as metatdata
		// Also (for now) try a simple attempt (might be broken) at returning "String" metadata
		// notes about the unsupported types that weren't loaded like sequences or string or
		// structure or opaque or object??
		Map<String,String> strings = new HashMap<>();
		List<Variable> vars = file.getVariables();
		for (Variable var : vars) {
			String dataType = var.getDataType().toString();
			String name = var.getNameAndDimensions();
			if (dataType.equals("char") || dataType.equals("String"))
			{
				String str = var.read().toString();
				strings.put(name, str);
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
				strings.put(name, "Unsupported data set of type " + dataType + " not loaded");
			}
			else
			{
				// note that some types might be newer than the netcdf code wae are using supports
				strings.put(name, "Unknown data set of type " + dataType + " ignored");
			}
		}
		return strings;
	}
	
	private <U extends Allocatable<U>>
		List<MultiDimDataSource<U>> readValues(NetcdfFile file, String[] types, U val, Procedure3<Array,Integer,U> assignProc)
	{
		List<Info> bandGroups = bandInfo(file, types);
		List<MultiDimDataSource<U>> datasets = new ArrayList<>();
		for (Info info : bandGroups) {
			MultiDimDataSource<U> ds = makeDataset(info, file, val);
			for (int i = 0; i < info.bandNums.size(); i++) {
				int band = info.bandNums.get(i);
				Variable var = file.getVariables().get(band);
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
				SamplingCartesianIntegerGrid grid =
						new SamplingCartesianIntegerGrid(minPt, maxPt);
				SamplingIterator<IntegerIndex> iter = grid.iterator();
				IntegerIndex index = new IntegerIndex(ds.numDimensions());
				int p = 0;
				while (iter.hasNext()) {
					assignProc.call(arr, p++, val);
					iter.next(index);
					ds.set(index, val);
				}
			}
			datasets.add(ds);
		}
		return datasets;
	}
	
	private List<MultiDimDataSource<UnsignedInt1Member>> readBools(NetcdfFile file) {
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
	
	private List<MultiDimDataSource<SignedInt8Member>> readBytes(NetcdfFile file) {
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
	
	private List<MultiDimDataSource<UnsignedInt8Member>> readUBytes(NetcdfFile file) {
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
	
	private List<MultiDimDataSource<SignedInt16Member>> readShorts(NetcdfFile file) {
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
	
	private List<MultiDimDataSource<UnsignedInt16Member>> readUShorts(NetcdfFile file) {
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
	
	private List<MultiDimDataSource<SignedInt32Member>> readInts(NetcdfFile file) {
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
	
	private List<MultiDimDataSource<UnsignedInt32Member>> readUInts(NetcdfFile file) {
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
	
	private List<MultiDimDataSource<SignedInt64Member>> readLongs(NetcdfFile file) {
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
	
	private List<MultiDimDataSource<UnsignedInt64Member>> readULongs(NetcdfFile file) {
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
	
	private List<MultiDimDataSource<Float32Member>> readFloats(NetcdfFile file) {
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
	
	private List<MultiDimDataSource<Float64Member>> readDoubles(NetcdfFile file) {
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

	private class Info {
		long size = 0;
		List<Integer> bandNums = new ArrayList<>();
	}
	
	private boolean contains(String[] keys, String key) {
		for (String k : keys) {
			if (key.equals(k))
				return true;
		}
		return false;
	}
	
	private List<Info> bandInfo(NetcdfFile file, String[] types) {
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
	
	private <U extends Allocatable<U>> MultiDimDataSource<U> makeDataset(Info info, NetcdfFile file, U type) {
		
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

		return MultiDimStorage.allocate(dimsStep5, type);
	}
	
	public static void main(String[] args)
	{
		String filename = "/home/bdz/Downloads/modis.hdf";
		NetcdfFile ncfile = null;
		try {
			ncfile = NetcdfFiles.open(filename);
			List<Dimension> dims = ncfile.getDimensions();
			System.out.println("file info: dims = "+dims.size());
			for (int i = 0; i < dims.size(); i++) {
				System.out.println("  "+dims.get(i).getLength());
			}
			List<Variable> bands = ncfile.getVariables();
			System.out.println("band info: count = "+bands.size());
			for (int i = 0; i < bands.size(); i++) {
				Variable band = bands.get(i);
				System.out.println("  band " + i);
				System.out.println("    type " + band.getDataType());
				System.out.println("    desc " + band.getDescription());
				System.out.println("    loc  " + band.getDatasetLocation());
				System.out.println("    dims " + band.getDimensionsString());
				System.out.println("    rank " + band.getRank());
				System.out.println("    shap " + Arrays.toString(band.getShape()));
				System.out.println("    size " + band.getSize());
				System.out.println("    unit " + band.getUnitsString());
				if (band.getDataType().toString().equals("char")) {
					Array arr = band.read();
					for (int j = 0; j < band.getSize(); j++) {
						if (j % 80 == 0)
							System.out.println();
						System.out.print(arr.getChar(j));
					}
					System.out.println();
				}
			}
			Tuple12<
			  Map<String,String>,
			  List<MultiDimDataSource<UnsignedInt1Member>>,
			  List<MultiDimDataSource<SignedInt8Member>>,
			  List<MultiDimDataSource<UnsignedInt8Member>>,
			  List<MultiDimDataSource<SignedInt16Member>>,
			  List<MultiDimDataSource<UnsignedInt16Member>>,
			  List<MultiDimDataSource<SignedInt32Member>>,
			  List<MultiDimDataSource<UnsignedInt32Member>>,
			  List<MultiDimDataSource<SignedInt64Member>>,
			  List<MultiDimDataSource<UnsignedInt64Member>>,
			  List<MultiDimDataSource<Float32Member>>,
			  List<MultiDimDataSource<Float64Member>>>
			data = new Main().loadDataSources(ncfile);
			for (MultiDimDataSource<?> ds : data.b()) {
				System.out.println("DS uint1");
				System.out.print("  ");
				for (int i = 0; i < ds.numDimensions(); i++) {
					if (i != 0)
						System.out.print(",");
					System.out.print(ds.dimension(i));
				}
				System.out.println();
			}
			for (MultiDimDataSource<?> ds : data.c()) {
				System.out.println("DS byte");
				System.out.print("  ");
				for (int i = 0; i < ds.numDimensions(); i++) {
					if (i != 0)
						System.out.print(",");
					System.out.print(ds.dimension(i));
				}
				System.out.println();
			}
			for (MultiDimDataSource<?> ds : data.d()) {
				System.out.println("DS ubyte");
				System.out.print("  ");
				for (int i = 0; i < ds.numDimensions(); i++) {
					if (i != 0)
						System.out.print(",");
					System.out.print(ds.dimension(i));
				}
				System.out.println();
			}
			for (MultiDimDataSource<?> ds : data.e()) {
				System.out.println("DS short");
				System.out.print("  ");
				for (int i = 0; i < ds.numDimensions(); i++) {
					if (i != 0)
						System.out.print(",");
					System.out.print(ds.dimension(i));
				}
				System.out.println();
			}
			for (MultiDimDataSource<?> ds : data.f()) {
				System.out.println("DS ushort");
				System.out.print("  ");
				for (int i = 0; i < ds.numDimensions(); i++) {
					if (i != 0)
						System.out.print(",");
					System.out.print(ds.dimension(i));
				}
				System.out.println();
			}
			for (MultiDimDataSource<?> ds : data.g()) {
				System.out.println("DS int");
				System.out.print("  ");
				for (int i = 0; i < ds.numDimensions(); i++) {
					if (i != 0)
						System.out.print(",");
					System.out.print(ds.dimension(i));
				}
				System.out.println();
			}
			for (MultiDimDataSource<?> ds : data.h()) {
				System.out.println("DS uint");
				System.out.print("  ");
				for (int i = 0; i < ds.numDimensions(); i++) {
					if (i != 0)
						System.out.print(",");
					System.out.print(ds.dimension(i));
				}
				System.out.println();
			}
			for (MultiDimDataSource<?> ds : data.i()) {
				System.out.println("DS long");
				System.out.print("  ");
				for (int i = 0; i < ds.numDimensions(); i++) {
					if (i != 0)
						System.out.print(",");
					System.out.print(ds.dimension(i));
				}
				System.out.println();
			}
			for (MultiDimDataSource<?> ds : data.j()) {
				System.out.println("DS ulong");
				System.out.print("  ");
				for (int i = 0; i < ds.numDimensions(); i++) {
					if (i != 0)
						System.out.print(",");
					System.out.print(ds.dimension(i));
				}
				System.out.println();
			}
			for (MultiDimDataSource<?> ds : data.k()) {
				System.out.println("DS float");
				System.out.print("  ");
				for (int i = 0; i < ds.numDimensions(); i++) {
					if (i != 0)
						System.out.print(",");
					System.out.print(ds.dimension(i));
				}
				System.out.println();
			}
			for (MultiDimDataSource<?> ds : data.l()) {
				System.out.println("DS double");
				System.out.print("  ");
				for (int i = 0; i < ds.numDimensions(); i++) {
					if (i != 0)
						System.out.print(",");
					System.out.print(ds.dimension(i));
				}
				System.out.println();
			}
			for (String key : data.a().keySet()) {
				System.out.println("CHARACTER DATA *******************************************");
				System.out.println("  key  = " + key);
				System.out.println("  text = " + data.a().get(key));
			}
		} catch (IOException e) {
			System.out.println("error trying to open " + filename);
		};
	}
	
}
