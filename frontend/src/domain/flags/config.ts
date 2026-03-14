export interface FlagConfig {
  moneyToleranceAbs: number;
  timeToleranceMinutes: number;
  pixTimeToleranceMinutes: number;
}

export const defaultFlagConfig: FlagConfig = {
  moneyToleranceAbs: 0.05,
  timeToleranceMinutes: 5,
  pixTimeToleranceMinutes: 120,
};
