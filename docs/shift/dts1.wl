(* ::Package:: *)

(* dts1.wl -- reference implementation of the dts1 timestamp-shift transform.

   This is the specification in executable form.  docs/TimestampShift.nb loads
   it, and the Python implementation (darpa_spillserver.shift) is tested
   against test vectors this file produces.

   Every quantity that decides an offset is computed exactly: integers for the
   flat variate, and exact rationals evaluated at 60 digits for the Gaussian.
*)

BeginPackage["DTS1`"];

$Ticks::usage = "NOvA clock ticks per second (64 MHz).";
$MaxOffset::usage = "Largest permitted offset bound, 500 ms in ticks.";
hmacSHA256::usage = "hmacSHA256[key, msg] for ByteArray key and String or ByteArray msg; returns a ByteArray.";
pbkdf2SHA256::usage = "pbkdf2SHA256[password, salt, iterations] returns the 32-byte derived key.";
paramString::usage = "paramString[params] is the canonical public part of a token.";
makeToken::usage = "makeToken[params, secret] builds the token string.";
parseToken::usage = "parseToken[token] returns <|params..., \"secret\" -> ByteArray|>.";
block::usage = "block[secret, P, source, n, j] is the j-th 32-byte keyed block for event n of source.";
as241::usage = "as241[p] is Wichura's AS241 PPND16 inverse normal CDF, evaluated in the precision of p.";
uniformFromWord::usage = "uniformFromWord[w] maps a 64-bit word to the exact dyadic u in (0, 1).";
offsetFlat::usage = "offsetFlat[secret, P, a, source, n] is the flat offset in ticks.";
offsetGauss::usage = "offsetGauss[secret, P, sigma, cut, source, n] returns {offset, x, draws}.";
offset::usage = "offset[token, source, n] is the offset in ticks for any dts1 token.";
fingerprint::usage = "fingerprint[token] is the public 16-hex-digit fingerprint of a token.";
shiftRow::usage = "shiftRow[tok, row] applies dts1 to an association with nova_time, source, event_number and optionally delta, pps_offset.";
invertRow::usage = "invertRow[tok, row] undoes shiftRow.";

Begin["`Private`"];

$Ticks = 64000000;
$MaxOffset = 32000000;

(* ---- HMAC-SHA256 (RFC 2104) -------------------------------------------- *)

sha256[b_List] := Normal[Hash[ByteArray[b], "SHA256", "ByteArray"]];

hmacSHA256[key_ByteArray, msg_String] := hmacSHA256[key, StringToByteArray[msg, "UTF-8"]];
hmacSHA256[key_ByteArray, msg_ByteArray] := Module[{k = Normal[key]},
  If[Length[k] > 64, k = sha256[k]];
  k = PadRight[k, 64];
  ByteArray[sha256[Join[BitXor[k, 16^^5c],
     sha256[Join[BitXor[k, 16^^36], Normal[msg]]]]]]];

(* PBKDF2 (RFC 8018) with HMAC-SHA256, one 32-byte block. *)
pbkdf2SHA256[password_String, salt_String, iterations_Integer] := Module[
  {key = StringToByteArray[password, "UTF-8"], u, acc},
  u = hmacSHA256[key, ByteArray[Join[Normal@StringToByteArray[salt, "UTF-8"], {0, 0, 0, 1}]]];
  acc = Normal[u];
  Do[u = hmacSHA256[key, u]; acc = BitXor[acc, Normal[u]], {iterations - 1}];
  ByteArray[acc]];

(* ---- tokens ------------------------------------------------------------ *)

paramString[p_Association] := Switch[p["dist"],
  "flat", "dts1:flat:a=" <> IntegerString[p["a"]],
  "gauss", "dts1:gauss:sigma=" <> IntegerString[p["sigma"]] <> ":cut=" <> IntegerString[p["cut"]]];

makeToken[p_Association, secret_ByteArray] :=
  paramString[p] <> ":" <> StringJoin[IntegerString[Normal[secret], 16, 2]];

parseToken[token_String] := Module[{f = StringSplit[token, ":"], kv},
  kv = Association[Rule @@@ (StringSplit[#, "="] & /@ f[[3 ;; -2]])];
  Join[<|"dist" -> f[[2]]|>, ToExpression /@ kv,
   <|"secret" -> ByteArray[FromDigits[#, 16] & /@ StringPartition[f[[-1]], 2]]|>]];

fingerprint[token_String] := Module[{p = parseToken[token]},
  StringTake[StringJoin[IntegerString[Normal@hmacSHA256[p["secret"],
       "dts1 fingerprint|" <> paramString[KeyDrop[p, "secret"]]], 16, 2]], 16]];

(* ---- keyed blocks ------------------------------------------------------ *)

(* Signed decimal: IntegerString alone drops the sign of a negative n. *)
intStr[n_Integer] := If[n < 0, "-" <> IntegerString[-n], IntegerString[n]];

block[secret_ByteArray, P_String, source_String, n_Integer, j_Integer] :=
  Normal@hmacSHA256[secret, P <> "|" <> source <> "|" <> intStr[n] <> "|" <> intStr[j]];

(* ---- flat variate ------------------------------------------------------ *)

offsetFlat[secret_, P_, a_Integer, source_, n_] :=
  Mod[FromDigits[Take[block[secret, P, source, n, 0], 16], 256], 2 a + 1] - a;

(* ---- AS241 PPND16 (Wichura 1988), coefficients as exact decimals --------- *)

(* Exact rational value of a decimal string such as "1.33e2" or "1". *)
exact[strs_List] := Map[Function[str, Module[{m, e, ip, fp},
     {m, e} = If[StringContainsQ[str, "e"], StringSplit[str, "e"], {str, "0"}];
     {ip, fp} = If[StringContainsQ[m, "."], StringSplit[m, ".", All], {m, ""}];
     FromDigits[ip <> fp] / 10^StringLength[fp] * 10^ToExpression[e]]], strs];

aC = exact[{"3.3871328727963666080e0", "1.3314166789178437745e2", "1.9715909503065514427e3",
    "1.3731693765509461125e4", "4.5921953931549871457e4", "6.7265770927008700853e4",
    "3.3430575583588128105e4", "2.5090809287301226727e3"}];
bC = exact[{"1", "4.2313330701600911252e1", "6.8718700749205790830e2", "5.3941960214247511077e3",
    "2.1213794301586595867e4", "3.9307895800092710610e4", "2.8729085735721942674e4",
    "5.2264952788528545610e3"}];
cC = exact[{"1.42343711074968357734e0", "4.63033784615654529590e0", "5.76949722146069140550e0",
    "3.64784832476320460504e0", "1.27045825245236838258e0", "2.41780725177450611770e-1",
    "2.27238449892691845833e-2", "7.74545014278341407640e-4"}];
dC = exact[{"1", "2.05319162663775882187e0", "1.67638483018380384940e0", "6.89767334985100004550e-1",
    "1.48103976427480074590e-1", "1.51986665636164571966e-2", "5.47593808499534494600e-4",
    "1.05075007164441684324e-9"}];
eC = exact[{"6.65790464350110377720e0", "5.46378491116411436990e0", "1.78482653991729133580e0",
    "2.96560571828504891230e-1", "2.65321895265761230930e-2", "1.24266094738807843860e-3",
    "2.71155556874348757815e-5", "2.01033439929228813265e-7"}];
fC = exact[{"1", "5.99832206555887937690e-1", "1.36929880922735805310e-1", "1.48753612908506148525e-2",
    "7.86869131145613259100e-4", "1.84631831751005468180e-5", "1.42151175831644588870e-7",
    "2.04426310338993978564e-15"}];

horner[c_List, r_] := Fold[#1 r + #2 &, 0, Reverse[c]];

as241[p_] := Module[{q = p - 1/2, r, x},
  If[Abs[q] <= 425/1000,
   r = 180625/1000000 - q^2;
   q horner[aC, r]/horner[bC, r],
   r = Sqrt[-Log[If[q < 0, p, 1 - p]]];
   x = If[r <= 5,
     r = r - 16/10; horner[cC, r]/horner[dC, r],
     r = r - 5; horner[eC, r]/horner[fC, r]];
   If[q < 0, -x, x]]];

uniformFromWord[w_Integer] := (BitShiftRight[w, 11] + 1/2)/2^53;

(* ---- Gaussian variate: AS241 of a uniform, rejection-truncated ---------- *)

offsetGauss[secret_, P_, sigma_Integer, cut_Integer, source_, n_] := Module[
  {b, w, x, draws = 0},
  Catch[
   Do[
    b = block[secret, P, source, n, j];
    Do[
     w = FromDigits[b[[8 i + 1 ;; 8 i + 8]], 256];
     draws++;
     x = N[as241[uniformFromWord[w]], 60];
     If[Abs[x] sigma <= cut, Throw[{Round[x sigma], x, draws}]],
     {i, 0, 3}],
    {j, 0, 63}];
   Throw[$Failed]]];

offset[token_String, source_String, n_Integer] := Module[{p = parseToken[token], P},
  P = paramString[KeyDrop[p, "secret"]];
  Switch[p["dist"],
   "flat", offsetFlat[p["secret"], P, p["a"], source, n],
   "gauss", First@offsetGauss[p["secret"], P, p["sigma"], p["cut"], source, n]]];

(* ---- row transform ----------------------------------------------------- *)

shiftRow[token_String, row_Association] := Module[
  {o = offset[token, row["source"], row["event_number"]], r = row},
  r["nova_time"] = row["nova_time"] + o;
  If[KeyExistsQ[row, "pps_offset"], r["pps_offset"] = Mod[row["pps_offset"] + o, $Ticks]];
  If[KeyExistsQ[row, "delta"],
   r["delta"] = row["delta"] + o - offset[token, row["source"], row["event_number"] - 1]];
  r];

invertRow[token_String, row_Association] := Module[
  {o = offset[token, row["source"], row["event_number"]], r = row},
  r["nova_time"] = row["nova_time"] - o;
  If[KeyExistsQ[row, "pps_offset"], r["pps_offset"] = Mod[row["pps_offset"] - o, $Ticks]];
  If[KeyExistsQ[row, "delta"],
   r["delta"] = row["delta"] - o + offset[token, row["source"], row["event_number"] - 1]];
  r];

End[];
EndPackage[];
