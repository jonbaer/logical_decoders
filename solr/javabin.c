#define PERL_NO_GET_CONTEXT
#include <EXTERN.h>
#include <perl.h>
#include <stdint.h>

// This layer of indirection allows us to call macros with macro arguments.
#define CALL(a,...) a(__VA_ARGS__)

#define hv_set(hv, key, val, hash) \
    Perl_hv_common(aTHX_ hv, NULL, STR_WITH_LEN(key), 0, HV_FETCH_ISSTORE, val, hash)

#define READ_LEN (in[-1] & 31) == 31 ? 31 + read_v_int() : in[-1] & 31

typedef union { uint64_t i; double d; } int_double;
typedef union { uint32_t i; float f; } int_float;

typedef struct {
    char    *key;
    uint8_t flags;
    uint32_t len;
} cached_key;

//TODO dynamically allocate cached keys.
static cached_key cached_keys[100];

static uint8_t cache_pos, *in, *out_buf;

// Computed at boot hash keys.
static uint32_t docs, maxScore, numFound, start;

static HV *bool_stash;
static SV *bool_true, *bool_false, *out_sv;

// Lucene variable-length +ve integer, the MSB indicates whether you need another octet.
// http://lucene.apache.org/core/old_versioned_docs/versions/3_5_0/fileformats.html#VInt
static uint32_t read_v_int() {
    uint8_t shift = 0;
    uint32_t result = 0;

    do result |= (*in++ & 127) << shift;
    while (in[-1] & 128 && (shift += 7));

    return result;
}

// This function reads the various JavaBin datatypes and returns a Perl SV.
// Different datatypes are jumped to view a lookup in an array of computed gotos.
//
// The first group (undef to enum) use the entire tag for the index of the type.
//
// The second are matched by taking the tag byte, shifting it by 5 so to only read
// the first 3 bits of the tag byte, giving it a range or 0-7 inclusive.
//
// To store both in one array the second group have 18 added to them. See DISPATCH.
//
// The remaining 5 bits can then be used to store the size of the datatype, e.g. how
// many chars in a string, this therefore has a range of 0-31, if the size exceeds or
// matches this then an additional vint is added.
//
// The overview of the tag byte is therefore TTTSSSSS with T and S being type and size.
static SV* read_sv(pTHX) {
    void* dispatch[] = {
        &&read_undef,
        &&read_bool,
        &&read_bool,
        &&read_byte,
        &&read_short,
        &&read_double,
        &&read_int,
        &&read_long,
        &&read_float,
        &&read_date,
        &&read_map,
        &&read_solr_doc,
        &&read_solr_doc_list,
        &&read_byte_array,
        &&read_iterator,
        NULL,
        NULL,
        NULL,
        &&read_enum,
        &&read_string,
        &&read_small_int,
        &&read_small_long,
        &&read_array,
        &&read_map,
        &&read_map,
    };

    in++;

    goto *dispatch[in[-1] >> 5 ? (in[-1] >> 5) + 18 : in[-1]];

read_undef:
    return &PL_sv_undef;
read_bool: {
        SV *rv = newSV_type(SVt_IV), *sv = in[-1] == 1 ? bool_true : bool_false;

        SvREFCNT(sv)++;
        SvROK_on(rv);
        SvRV_set(rv, sv);

        return rv;
    }
read_byte:
    return newSViv((int8_t) *in++);
read_short: {
        const int16_t s = in[0] << 8 | in[1];

        in += 2;

        return newSViv(s);
    }
read_double: {
        // For perls with double length NVs this conversion is simple.
        // Read 8 bytes, cast to double, return. For long double perls
        // more magic is used, see read_float for more details.

        const int_double u = { (uint64_t) in[0] << 56 |
                               (uint64_t) in[1] << 48 |
                               (uint64_t) in[2] << 40 |
                               (uint64_t) in[3] << 32 |
                               (uint64_t) in[4] << 24 |
                               (uint64_t) in[5] << 16 |
                               (uint64_t) in[6] << 8  |
                               (uint64_t) in[7] };

        in += 8;

    #ifdef USE_LONG_DOUBLE
        char *str = alloca(snprintf(NULL, 0, "%.14f", u.d));

        sprintf(str, "%.14f", u.d);

        return newSVnv(strtold(str, NULL));
    #else
        return newSVnv(u.d);
    #endif
    }
read_int: {
        const int32_t i = in[0] << 24 | in[1] << 16 | in[2] << 8 | in[3];

        in += 4;

        return newSViv(i);
    }
read_long: {
        const int64_t l = (uint64_t) in[0] << 56 |
                          (uint64_t) in[1] << 48 |
                          (uint64_t) in[2] << 40 |
                          (uint64_t) in[3] << 32 |
                          (uint64_t) in[4] << 24 |
                          (uint64_t) in[5] << 16 |
                          (uint64_t) in[6] << 8  |
                          (uint64_t) in[7];

        in += 8;

        return newSViv(l);
    }
read_float: {
        // JavaBin has a 4byte float format, NVs in perl are double or long double,
        // therefore a little magic is required. Read the 4 bytes into an int in the
        // correct endian order. Re-read these bits as a float, stringify this float,
        // then finally numify the string into a double or long double.
        const int_float u = { in[0] << 24 | in[1] << 16 | in[2] << 8 | in[3] };

        in += 4;

        char *str = alloca(snprintf(NULL, 0, "%f", u.f));

        sprintf(str, "%f", u.f);

    #ifdef USE_LONG_DOUBLE
        return newSVnv(strtold(str, NULL));
    #else
        return newSVnv(strtod(str, NULL));
    #endif
    }
read_date: {
        const int64_t date_ms = (uint64_t) in[0] << 56 |
                                (uint64_t) in[1] << 48 |
                                (uint64_t) in[2] << 40 |
                                (uint64_t) in[3] << 32 |
                                (uint64_t) in[4] << 24 |
                                (uint64_t) in[5] << 16 |
                                (uint64_t) in[6] << 8  |
                                (uint64_t) in[7];

        in += 8;

        const time_t date = date_ms / 1000;

        const struct tm *t = gmtime(&date);

        char date_str[25];

        sprintf(date_str, "%u-%02u-%02uT%02u:%02u:%02u.%03uZ", t->tm_year + 1900,
                t->tm_mon + 1,
                t->tm_mday,
                t->tm_hour,
                t->tm_min,
                t->tm_sec,
                (uint32_t) (date_ms % 1000));

        return newSVpvn(date_str, 24);
    }
read_solr_doc:
    in++;     // Assume a solr doc is a map.
read_map: {
        HV *hv = (HV*)newSV_type(SVt_PVHV);

        uint32_t len = in[-1] >> 5 ? READ_LEN : read_v_int();

        while (len--) {
            cached_key key;

            in++;

            const uint32_t i = READ_LEN;

            if (i)
                key = cached_keys[i];
            else {
                in++;

                cached_keys[++cache_pos] = key = (cached_key){ (char*)in, 0, READ_LEN };

                uint8_t *key_str = in;

                in += key.len;

                // Set the UTF8 flag if we hit a high byte.
                while (key_str != in) {
                    if (*key_str++ & 128) {
                        key.flags = HVhek_UTF8;
                        break;
                    }
                }
            }

            hv_common(hv, NULL, key.key, key.len, key.flags, HV_FETCH_ISSTORE, read_sv(aTHX), 0);
        }

        SV *rv = newSV_type(SVt_IV);

        SvROK_on(rv);
        SvRV_set(rv, (SV*)hv);

        return rv;
    }
read_solr_doc_list: {
        HV *hv = (HV*)newSV_type(SVt_PVHV);

        // Assume values are in an array, skip tag & read_sv.
        in++;

        hv_set(hv, "numFound", read_sv(aTHX), numFound);
        hv_set(hv, "start",    read_sv(aTHX), start);
        hv_set(hv, "maxScore", read_sv(aTHX), maxScore);
        hv_set(hv, "docs",     read_sv(aTHX), docs);

        SV *rv = newSV_type(SVt_IV);

        SvROK_on(rv);
        SvRV_set(rv, (SV*)hv);

        return rv;
    }
read_byte_array: {
        AV *av = (AV*)newSV_type(SVt_PVAV);

        SSize_t len = read_v_int();

        SV **ary = safemalloc(len * sizeof(SV*));

        AvALLOC(av) = AvARRAY(av) = ary;
        AvFILLp(av) = AvMAX(av) = len - 1;

        while (len--)
            *ary++ = newSViv((int8_t) *in++);

        SV *rv = newSV_type(SVt_IV);

        SvROK_on(rv);
        SvRV_set(rv, (SV*)av);

        return rv;
    }
read_iterator: {
        AV *av = (AV*)newSV_type(SVt_PVAV);

        uint32_t len = 0;

        while (*in != 15)
            av_store(av, len++, read_sv(aTHX));

        in++;

        SV *rv = newSV_type(SVt_IV);

        SvROK_on(rv);
        SvRV_set(rv, (SV*)av);

        return rv;
    }
read_enum: {
        SV *sv = read_sv(aTHX); // small_int if +ve, int otherwise.

        sv_upgrade(sv, SVt_PVMG);

        in++;

        const STRLEN len = READ_LEN;

        char *str = sv_grow(sv, len + 1);

        memcpy(str, in, len);

        in += len;

        str[len] = '\0';

        SvCUR(sv) = len;

        SvFLAGS(sv) = SVf_IOK | SVp_IOK | SVs_OBJECT | SVf_POK | SVp_POK | SVt_PVMG | SVf_UTF8;

        HV *stash = CALL(gv_stashpvn, STR_WITH_LEN("JavaBin::Enum"), 0);

        SvREFCNT(stash)++;
        SvSTASH_set(sv, stash);

        SV *rv = newSV_type(SVt_IV);

        SvROK_on(rv);
        SvRV_set(rv, sv);

        return rv;
    }
read_string: {
        const STRLEN len = READ_LEN;

        SV *sv = newSV_type(SVt_PV);

        char *str = SvPVX(sv) = (char*)safemalloc(len);

        memcpy(str, in, len);

        SvCUR(sv) = SvLEN(sv) = len;
        SvFLAGS(sv) |= SVf_POK | SVp_POK | SVf_UTF8;

        in += len;

        return sv;
    }
read_small_int: {
        uint32_t result = in[-1] & 15;

        if (in[-1] & 16)
            result |= read_v_int() << 4;

        return newSViv(result);
    }
read_small_long: {
        uint64_t result = in[-1] & 15;

        // Inlined variable-length +ve long code, see read_v_int().
        if (in[-1] & 16) {
            uint8_t shift = 4;

            do result |= (*in++ & 127) << shift;
            while (in[-1] & 128 && (shift += 7));
        }

        return newSViv(result);
    }
read_array: {
        AV *av = (AV*)newSV_type(SVt_PVAV);

        SSize_t len = READ_LEN;

        SV **ary = safemalloc(len * sizeof(SV*));

        AvALLOC(av) = AvARRAY(av) = ary;
        AvFILLp(av) = AvMAX(av) = len - 1;

        while (len--)
            *ary++ = read_sv(aTHX);

        SV *rv = newSV_type(SVt_IV);

        SvROK_on(rv);
        SvRV(rv) = (SV*)av;

        return rv;
    }
}

static void grow_out(pTHX_ const STRLEN want) {
    const STRLEN len = out_buf - (uint8_t *)SvPVX(out_sv);

    // If we want more than we have, realloc the string.
    if (len + want >= SvLEN(out_sv)) {
        sv_grow(out_sv, len + want);

        out_buf = (uint8_t *)SvPVX(out_sv) + len;
    }
}

static void write_v_int(uint32_t i) {
    while (i & ~127) {
        *out_buf++ = (i & 127) | 128;

        i >>= 7;
    }

    *out_buf++ = i;
}

static void write_shifted_tag(uint8_t tag, uint32_t len) {
    if (len < 31)
        *out_buf++ = tag | len;
    else {
        *out_buf++ = tag | 31;

        write_v_int(len - 31);
    }
}

static void write_sv(pTHX_ SV *sv) {
    SvGETMAGIC(sv);

    if (SvPOKp(sv)) {
        const STRLEN len = SvCUR(sv);

        grow_out(aTHX_ len + 5);

        write_shifted_tag(32, len);

        memcpy(out_buf, SvPVX(sv), len);

        out_buf += len;
    }
    else if (SvNOKp(sv)) {
        const int_double u = { .d = SvNV(sv) };

        grow_out(aTHX_ 9);

        *out_buf++ = 5;
        *out_buf++ = u.i >> 56;
        *out_buf++ = u.i >> 48;
        *out_buf++ = u.i >> 40;
        *out_buf++ = u.i >> 32;
        *out_buf++ = u.i >> 24;
        *out_buf++ = u.i >> 16;
        *out_buf++ = u.i >> 8;
        *out_buf++ = u.i;
    }
    else if (SvIOKp(sv)) {
        const int64_t i = SvIV(sv);

        if (i == (int8_t)i) {
            grow_out(aTHX_ 2);

            *out_buf++ = 3;
            *out_buf++ = i;
        }
        else if (i == (int16_t)i) {
            grow_out(aTHX_ 3);

            *out_buf++ = 4;
            *out_buf++ = i >> 8;
            *out_buf++ = i;
        }
        else if (i == (int32_t)i) {
            grow_out(aTHX_ 5);

            *out_buf++ = 6;
            *out_buf++ = i >> 24;
            *out_buf++ = i >> 16;
            *out_buf++ = i >> 8;
            *out_buf++ = i;
        }
        else {
            grow_out(aTHX_ 9);

            *out_buf++ = 7;
            *out_buf++ = i >> 56;
            *out_buf++ = i >> 48;
            *out_buf++ = i >> 40;
            *out_buf++ = i >> 32;
            *out_buf++ = i >> 24;
            *out_buf++ = i >> 16;
            *out_buf++ = i >> 8;
            *out_buf++ = i;
        }
    }
    else if (SvROK(sv)) {
        sv = SvRV(sv);

        // If we have a JavaBin::Bool.
        if (SvTYPE(sv) == SVt_IV || SvSTASH(sv) == bool_stash) {
            grow_out(aTHX_ 1);

            *out_buf++ = SvIV(sv) ? 1 : 2;

            return;
        }

        switch (SvTYPE(sv)) {
        case SVt_PVAV: {
            SSize_t len = AvFILLp(sv) + 1;

            grow_out(aTHX_ len + 5);

            write_shifted_tag(128, len);

            SV **ary = AvARRAY(sv);

            while (len--)
                write_sv(aTHX_ *ary++);

            break;
        }
        case SVt_PVHV: {
            *out_buf++ = 10;

            uint32_t len;

            if ((len = HvUSEDKEYS(sv))) {
                grow_out(aTHX_ 4);

                write_v_int(len);

                HE **start = HvARRAY(sv), **end = start + HvMAX(sv) + 1;

                do {
                    HE *entry;

                    for (entry = *start++; entry; entry = HeNEXT(entry)) {
                        SV *value = HeVAL(entry);

                        if (value != &PL_sv_placeholder) {
                            STRLEN klen = HeKLEN(entry);

                            grow_out(aTHX_ klen + 6);

                            //TODO Implement the cached key feature.
                            *out_buf++ = 0;

                            write_shifted_tag(32, klen);

                            memcpy(out_buf, HeKEY(entry), klen);

                            out_buf += klen;

                            write_sv(aTHX_ value);

                            if (--len == 0)
                                return;
                        }
                    }
                } while (start != end);
            }
            else {
                grow_out(aTHX_ 1);

                *out_buf++ = 0;
            }

            break;
        }
        default:
            grow_out(aTHX_ 1);

            *out_buf++ = 0;
        }
    }
    else {
        grow_out(aTHX_ 1);

        *out_buf++ = 0;
    }
}

static void from_javabin(pTHX_ CV *cv __attribute__((unused))) {
    SV **sp = PL_stack_base + *PL_markstack_ptr + 1;

    if (sp > PL_stack_sp)
        return;

    // Zero the cache.
    // TODO zero more than just the cache index?
    cache_pos = 0;

    if (SvCUR(*sp) < 2)
        croak("Invalid from_javabin input: insufficient length");

    in = (uint8_t *) SvPVX(*sp);

    if (*in++ != 2)
        croak("Invalid from_javabin input: expected version 2");

    *sp = sv_2mortal(read_sv(aTHX));

    PL_stack_sp = sp;
}

static void to_javabin(pTHX_ CV *cv __attribute__((unused))) {
    SV **sp = PL_stack_base + *PL_markstack_ptr + 1;

    out_sv = PAD_SV(PL_op->op_targ);

    sv_upgrade(out_sv, SVt_PV);
    SvPOK_on(out_sv);

    out_buf = (uint8_t *)SvPVX(out_sv);

    grow_out(aTHX_ 1);

    *out_buf++ = 2;

    write_sv(aTHX_ *sp);

    SvCUR(out_sv) = out_buf - (uint8_t *)SvPVX(out_sv);

    *sp = out_sv;

    PL_stack_sp = sp;
}

static void deref(pTHX_ CV *cv __attribute__((unused))) {
    PL_stack_sp = PL_stack_base + *PL_markstack_ptr + 1;

    *PL_stack_sp = SvRV(*PL_stack_sp);
}

static void sub(pTHX_ char *name, STRLEN len, XSUBADDR_t addr) {
    CV *cv = (CV*)newSV_type(SVt_PVCV);
    GV *gv = gv_fetchpvn_flags(name, len, GV_ADD, SVt_PVCV);

    CvISXSUB_on(cv);
    CvXSUB(cv) = addr;

    CvGV_set(cv, gv);
    GvCV_set(gv, cv);

    SvFLAGS(GvSTASH(gv)) |= SVf_AMAGIC;
}

void boot_JavaBin(pTHX_ CV *cv __attribute__((unused))) {
    sub(aTHX_ STR_WITH_LEN("JavaBin::from_javabin"), from_javabin);
    sub(aTHX_ STR_WITH_LEN("JavaBin::to_javabin"), to_javabin);
    sub(aTHX_ STR_WITH_LEN("JavaBin::Bool::()"), NULL);
    sub(aTHX_ STR_WITH_LEN("JavaBin::Bool::(bool"), deref);
    sub(aTHX_ STR_WITH_LEN("JavaBin::Enum::()"), NULL);
    sub(aTHX_ STR_WITH_LEN("JavaBin::Enum::(0+"), deref);
    sub(aTHX_ STR_WITH_LEN("JavaBin::Enum::(\"\""), deref);

    sv_setsv_flags(
        GvSV(CALL(gv_fetchpvn_flags, STR_WITH_LEN("JavaBin::Bool::()"), GV_ADD, SVt_PV)),
        &PL_sv_yes,
        0
        );
    sv_setsv_flags(
        GvSV(CALL(gv_fetchpvn_flags, STR_WITH_LEN("JavaBin::Enum::()"), GV_ADD, SVt_PV)),
        &PL_sv_yes,
        0
        );

    // Make two bools (true and false), store them in globals.
    bool_stash = CALL(gv_stashpvn, STR_WITH_LEN("JavaBin::Bool"), 0);
    SvREFCNT(bool_stash) += 2;

    bool_true  = newSViv(1);
    bool_false = newSViv(0);

    sv_upgrade(bool_true,  SVt_PVMG);
    sv_upgrade(bool_false, SVt_PVMG);

    SvOBJECT_on(bool_true);
    SvOBJECT_on(bool_false);

    // sv_setsv_flags will set these back to 1.
    SvREFCNT(bool_true) = SvREFCNT(bool_false) = 0;

    SvSTASH(bool_true) = SvSTASH(bool_false) = bool_stash;

    // Take refs to the bool and store them on the JavaBin pkg.
    SV *sv = newSV_type(SVt_IV);

    SvROK_on(sv);
    SvRV(sv) = bool_true;

    sv_setsv_flags(
        GvSV(CALL(gv_fetchpvn_flags, STR_WITH_LEN("JavaBin::true"), GV_ADD, SVt_PV)),
        sv,
        0
        );

    sv = newSV_type(SVt_IV);

    SvROK_on(sv);
    SvRV(sv) = bool_false;

    sv_setsv_flags(
        GvSV(CALL(gv_fetchpvn_flags, STR_WITH_LEN("JavaBin::false"), GV_ADD, SVt_PV)),
        sv,
        0
        );

    // FIXME Could these be done at compile time? Or is it impossible given
    // different hashing algos?
    // Precompute some hash keys.
    PERL_HASH(docs,     "docs",     4);
    PERL_HASH(maxScore, "maxScore", 8);
    PERL_HASH(numFound, "numFound", 8);
    PERL_HASH(start,    "start",    5);
}
