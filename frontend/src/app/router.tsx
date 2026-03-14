import { createBrowserRouter } from 'react-router-dom';
import { Shell } from '../components/Layout/Shell';
import { Dashboard } from '../pages/Dashboard';
import { Reconciliation } from '../pages/Reconciliation';
import { ImportFiles } from '../pages/ImportFiles';
import { AuditDuplicates } from '../pages/AuditDuplicates';
import { UsersAdmin } from '../pages/UsersAdmin';

export const router = createBrowserRouter([
  {
    path: '/',
    element: <Shell />,
    children: [
      { index: true, element: <Dashboard /> },
      { path: 'reconciliation', element: <Reconciliation /> },
      { path: 'audit-duplicates', element: <AuditDuplicates /> },
      { path: 'imports', element: <ImportFiles /> },
      { path: 'interdata', element: <ImportFiles /> },
      { path: 'cielo-edi', element: <ImportFiles /> },
      { path: 'users', element: <UsersAdmin /> },
    ],
  },
]);
