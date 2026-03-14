export const isDebug = import.meta.env.VITE_DEBUG === 'true';

export const dlog = (...args: unknown[]) => {
  if (isDebug) {
    console.log(...args);
  }
};

export const derr = (...args: unknown[]) => {
  if (isDebug) {
    console.error(...args);
  }
};
