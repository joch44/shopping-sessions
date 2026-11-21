//! CPU guard. This binary is built for Cascade Lake (AVX-512), and so is /fallback. On a host
//! without the features the code uses, the first AVX-512 instruction would kill the process
//! (SIGILL) and the run would produce nothing. Before any Rust code runs (an `.init_array`
//! entry, called by musl after its own baseline-compiled startup), plain x86-64 assembly checks
//! CPUID and XCR0 for every extension the build emits (checked with objdump: AVX-512
//! F/BW/VL/DQ/CD, AVX2, BMI1/2, LZCNT, POPCNT, MOVBE, SSE4.2). If one is missing, it execs a
//! general-path build that runs there: /fallback-v3 (x86-64-v3, AVX2) when the CPU has AVX2,
//! else /fallback-v2 (x86-64-v2). Both read DATA_DIR / OUTPUT_DIR like /fallback.
//! Cost on a capable CPU: a few CPUID instructions.

core::arch::global_asm!(
    ".pushsection .text.xl_cpu_guard,\"ax\",@progbits",
    ".globl xl_cpu_guard",
    "xl_cpu_guard:",
    "push rbx",
    // Max basic leaf >= 7.
    "xor eax, eax",
    "xor ecx, ecx",
    "cpuid",
    "cmp eax, 7",
    "jb 8f",
    // Leaf 1 ECX: SSE3, SSSE3, SSE4.1, SSE4.2, MOVBE, POPCNT, OSXSAVE, AVX -> r8d;
    // the x86-64-v3 set (adds FMA, CX16, F16C) -> r9d.
    "mov eax, 1",
    "xor ecx, ecx",
    "cpuid",
    "mov r8d, ecx",
    "and r8d, 0x18D80201",
    "cmp r8d, 0x18D80201",
    "sete r8b",
    "movzx r8d, r8b",
    "mov r9d, ecx",
    "and r9d, 0x38D83201",
    "cmp r9d, 0x38D83201",
    "sete r9b",
    "movzx r9d, r9b",
    // XCR0 (OSXSAVE checked above for both): SSE+AVX state (0x6), plus opmask/ZMM (0xE0).
    "test ecx, 0x08000000",
    "jz 8f",
    "xor ecx, ecx",
    "xgetbv",
    "mov r10d, eax",
    "and r10d, 0x6",
    "cmp r10d, 0x6",
    "jne 8f",
    "and eax, 0xE6",
    "cmp eax, 0xE6",
    "je 2f",
    "xor r8d, r8d",
    "2:",
    // Leaf 7 EBX: BMI1, AVX2, BMI2, AVX512F, DQ, CD, BW, VL (fast) / BMI1, AVX2, BMI2 (v3).
    "mov eax, 7",
    "xor ecx, ecx",
    "cpuid",
    "mov eax, ebx",
    "and eax, 0xD0030128",
    "cmp eax, 0xD0030128",
    "je 3f",
    "xor r8d, r8d",
    "3:",
    "and ebx, 0x128",
    "cmp ebx, 0x128",
    "je 4f",
    "xor r9d, r9d",
    "4:",
    // Extended leaf 0x80000001 ECX bit 5: LZCNT (both sets).
    "mov eax, 0x80000000",
    "cpuid",
    "cmp eax, 0x80000001",
    "jb 8f",
    "mov eax, 0x80000001",
    "xor ecx, ecx",
    "cpuid",
    "test ecx, 0x20",
    "jz 8f",
    "test r8d, r8d",
    "jz 9f",
    "pop rbx",
    "ret",
    // Not this build's CPU: AVX2 general path if possible, else the x86-64-v2 one.
    "9:",
    "test r9d, r9d",
    "jz 8f",
    "lea rdi, [rip + xl_guard_v3]",
    "lea rsi, [rip + xl_guard_argv3]",
    "jmp 7f",
    "8:",
    "lea rdi, [rip + xl_guard_v2]",
    "lea rsi, [rip + xl_guard_argv2]",
    "7:",
    "mov rdx, qword ptr [rip + __environ]",
    "mov eax, 59", // execve
    "syscall",
    "mov edi, 3", // exec failed: exit_group(3)
    "mov eax, 231",
    "syscall",
    "ud2",
    ".popsection",
    ".pushsection .rodata.xl_cpu_guard,\"a\",@progbits",
    "xl_guard_v3: .asciz \"/fallback-v3\"",
    "xl_guard_v2: .asciz \"/fallback-v2\"",
    ".popsection",
    ".pushsection .data.rel.ro.xl_cpu_guard,\"aw\",@progbits",
    ".balign 8",
    "xl_guard_argv3: .quad xl_guard_v3, 0",
    "xl_guard_argv2: .quad xl_guard_v2, 0",
    ".popsection",
);

extern "C" {
    fn xl_cpu_guard();
}

#[used]
#[link_section = ".init_array"]
static XL_CPU_GUARD: unsafe extern "C" fn() = xl_cpu_guard;
