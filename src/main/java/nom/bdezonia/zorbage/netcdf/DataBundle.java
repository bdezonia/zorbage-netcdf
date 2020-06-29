/*
 * zorbage-netcdf: code for using the NetCDF data file library to open files into zorbage data structures for further processing
 *
 * Copyright (C) 2020 Barry DeZonia
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package nom.bdezonia.zorbage.netcdf;

import java.util.List;
import java.util.Map;

import nom.bdezonia.zorbage.data.DimensionedDataSource;
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

/**
 * @author Barry DeZonia
 */
public class DataBundle {
	public Map<String,String> chars;
	public List<DimensionedDataSource<UnsignedInt1Member>> int1s;
	public List<DimensionedDataSource<SignedInt8Member>> int8s;
	public List<DimensionedDataSource<UnsignedInt8Member>> uint8s;
	public List<DimensionedDataSource<SignedInt16Member>> int16s;
	public List<DimensionedDataSource<UnsignedInt16Member>> uint16s;
	public List<DimensionedDataSource<SignedInt32Member>> int32s;
	public List<DimensionedDataSource<UnsignedInt32Member>> uint32s;
	public List<DimensionedDataSource<SignedInt64Member>> int64s;
	public List<DimensionedDataSource<UnsignedInt64Member>> uint64s;
	public List<DimensionedDataSource<Float32Member>> floats;
	public List<DimensionedDataSource<Float64Member>> doubles;
}
