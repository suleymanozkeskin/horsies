// The spy on `process.dlopen` of a seat isolate fixture: it records the file of each native module load in this
// thread. The entry imports it first, so the spy is in place before any module of the package runs.

/** The file of each `process.dlopen` call in this thread. */
export const dlopenFiles: string[] = [];

const original = process.dlopen.bind(process);

process.dlopen = (module, filename, flags) => {
  dlopenFiles.push(filename);

  return flags === undefined ? original(module, filename) : original(module, filename, flags);
};
